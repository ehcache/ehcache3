/*
 * Copyright Terracotta, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.ehcache.clustered.server;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.exceptions.ClusterException;
import org.ehcache.clustered.common.internal.exceptions.IllegalMessageException;
import org.ehcache.clustered.common.internal.exceptions.InvalidOperationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.exceptions.ResourceBusyException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponseFactory;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.ReconnectDataCodec;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.messages.StateRepositoryOpMessage;
import org.ehcache.clustered.common.internal.store.ServerStore;
import org.ehcache.clustered.server.state.EhcacheStateService;
import org.ehcache.clustered.server.state.config.EhcacheStateServiceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapresource.OffHeapResources;

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.allInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateAll;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.hashInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.serverInvalidateHash;

import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.ConfigureStoreManager;
import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.CreateServerStore;
import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.DestroyServerStore;
import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.ReleaseServerStore;
import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateServerStore;
import static org.ehcache.clustered.common.internal.messages.LifecycleMessage.ValidateStoreManager;

// TODO: Provide some mechanism to report on storage utilization -- PageSource provides little visibility
// TODO: Ensure proper operations for concurrent requests
class EhcacheActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheActiveEntity.class);

  private final UUID identity;

  /**
   * Tracks the state of a connected client.  An entry is added to this map when the
   * {@link #connected(ClientDescriptor)} method is invoked for a client and removed when the
   * {@link #disconnected(ClientDescriptor)} method is invoked for the client.
   */
  private final Map<ClientDescriptor, ClientState> clientStateMap = new HashMap<ClientDescriptor, ClientState>();

  private final ConcurrentHashMap<String, Set<ClientDescriptor>> storeClientMap =
      new ConcurrentHashMap<String, Set<ClientDescriptor>>();

  private final ReconnectDataCodec reconnectDataCodec = new ReconnectDataCodec();
  private final ServerStoreCompatibility storeCompatibility = new ServerStoreCompatibility();
  private final EhcacheEntityResponseFactory responseFactory;
  private final ConcurrentMap<Integer, InvalidationHolder> clientsWaitingForInvalidation = new ConcurrentHashMap<Integer, InvalidationHolder>();
  private final AtomicInteger invalidationIdGenerator = new AtomicInteger();
  private final ClientCommunicator clientCommunicator;
  private final EhcacheStateService ehcacheStateService;

  static class InvalidationHolder {
    final ClientDescriptor clientDescriptorWaitingForInvalidation;
    final Set<ClientDescriptor> clientsHavingToInvalidate;
    final String cacheId;
    final Long key;

    InvalidationHolder(ClientDescriptor clientDescriptorWaitingForInvalidation, Set<ClientDescriptor> clientsHavingToInvalidate, String cacheId, Long key) {
      this.clientDescriptorWaitingForInvalidation = clientDescriptorWaitingForInvalidation;
      this.clientsHavingToInvalidate = clientsHavingToInvalidate;
      this.cacheId = cacheId;
      this.key = key;
    }

    InvalidationHolder(ClientDescriptor clientDescriptorWaitingForInvalidation, Set<ClientDescriptor> clientsHavingToInvalidate, String cacheId) {
      this.clientDescriptorWaitingForInvalidation = clientDescriptorWaitingForInvalidation;
      this.clientsHavingToInvalidate = clientsHavingToInvalidate;
      this.cacheId = cacheId;
      this.key = null;
    }
  }

  private static class CommunicatorServiceConfiguration implements ServiceConfiguration<ClientCommunicator> {
    @Override
    public Class<ClientCommunicator> getServiceType() {
      return ClientCommunicator.class;
    }
  }

  private static class OffHeapResourcesServiceConfiguration implements ServiceConfiguration<OffHeapResources> {

    @Override
    public Class<OffHeapResources> getServiceType() {
      return OffHeapResources.class;
    }

  }

  EhcacheActiveEntity(ServiceRegistry services, byte[] config) {
    this.identity = ClusteredEhcacheIdentity.deserialize(config);
    this.responseFactory = new EhcacheEntityResponseFactory();
    this.clientCommunicator = services.getService(new CommunicatorServiceConfiguration());
    ehcacheStateService = services.getService(new EhcacheStateServiceConfig(services));
    if (ehcacheStateService == null) {
      throw new AssertionError("Server failed to retrieve EhcacheStateService.");
    }
  }

  /**
   * Gets the map of connected clients along with the server stores each is using.
   * If the client is using no stores, the set of stores will be empty for that client.
   *
   * @return an unmodifiable copy of the connected client map
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  Map<ClientDescriptor, Set<String>> getConnectedClients() {
    final HashMap<ClientDescriptor, Set<String>> clientMap = new HashMap<ClientDescriptor, Set<String>>();
    for (Entry<ClientDescriptor, ClientState> entry : clientStateMap.entrySet()) {
      clientMap.put(entry.getKey(), entry.getValue().getAttachedStores());
    }
    return Collections.unmodifiableMap(clientMap);
  }

  /**
   * Gets the map of in-use stores along with the clients using each.
   * This map is not expected to hold a store with no active client.
   *
   * @return an unmodifiable copy of the in-use store map
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  Map<String, Set<ClientDescriptor>> getInUseStores() {
    final HashMap<String, Set<ClientDescriptor>> storeMap = new HashMap<String, Set<ClientDescriptor>>();
    for (Map.Entry<String, Set<ClientDescriptor>> entry : storeClientMap.entrySet()) {
      storeMap.put(entry.getKey(), Collections.unmodifiableSet(new HashSet<ClientDescriptor>(entry.getValue())));
    }
    return Collections.unmodifiableMap(storeMap);
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    if (!clientStateMap.containsKey(clientDescriptor)) {
      LOGGER.info("Connecting {}", clientDescriptor);
      clientStateMap.put(clientDescriptor, new ClientState());
    } else {
      // This is logically an AssertionError
      LOGGER.error("Client {} already registered as connected", clientDescriptor);
    }
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    // cleanup all invalidation requests waiting for a ack from this client
    Set<Integer> invalidationIds = clientsWaitingForInvalidation.keySet();
    for (Integer invalidationId : invalidationIds) {
      clientInvalidated(clientDescriptor, invalidationId);
    }

    // cleanup all invalidation request this client was blocking on
    Iterator<Entry<Integer, InvalidationHolder>> it = clientsWaitingForInvalidation.entrySet().iterator();
    while (it.hasNext()) {
      Entry<Integer, InvalidationHolder> next = it.next();
      if (next.getValue().clientDescriptorWaitingForInvalidation.equals(clientDescriptor)) {
        it.remove();
      }
    }

    ClientState clientState = clientStateMap.remove(clientDescriptor);
    if (clientState == null) {
      // This is logically an AssertionError
      LOGGER.error("Client {} not registered as connected", clientDescriptor);
    } else {
      LOGGER.info("Disconnecting {}", clientDescriptor);
      for (String storeId : clientState.getAttachedStores()) {
        detachStore(clientDescriptor, storeId);
      }
    }
  }

  @Override
  public EhcacheEntityResponse invoke(ClientDescriptor clientDescriptor, EhcacheEntityMessage message) {
    try {
      switch (message.getType()) {
        case LIFECYCLE_OP:
          return invokeLifeCycleOperation(clientDescriptor, (LifecycleMessage) message);
        case SERVER_STORE_OP:
          return invokeServerStoreOperation(clientDescriptor, (ServerStoreOpMessage) message);
        case STATE_REPO_OP:
          return invokeStateRepositoryOperation(clientDescriptor, (StateRepositoryOpMessage) message);
        default:
          throw new IllegalMessageException("Unknown message : " + message);
      }
    } catch (ClusterException e) {
      return responseFactory.failure(e);
    } catch (Exception e) {
      LOGGER.error("Unexpected exception raised during operation: " + message, e);
      return responseFactory.failure(new InvalidOperationException(e));
    }
  }

  @Override
  public void handleReconnect(ClientDescriptor clientDescriptor, byte[] extendedReconnectData) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new AssertionError("Client "+ clientDescriptor +" trying to reconnect is not connected to entity");
    }
    clientState.attach();
    Set<String> cacheIds = reconnectDataCodec.decode(extendedReconnectData);
    for (final String cacheId : cacheIds) {
      ServerStoreImpl serverStore = ehcacheStateService.getStore(cacheId);
      if (serverStore == null) {
        //Client removes the cache's reference only when destroy has successfully completed
        //This happens only when client thinks destroy is still not complete
        LOGGER.warn("ServerStore '{}' does not exist as expected by Client '{}'.", cacheId, clientDescriptor);
        continue;
      }
      serverStore.setEvictionListener(new ServerStoreEvictionListener() {
        @Override
        public void onEviction(long key) {
          invalidateHashAfterEviction(cacheId, key);
        }
      });
      attachStore(clientDescriptor, cacheId);
    }
    LOGGER.info("Client '{}' successfully reconnected to newly promoted ACTIVE after failover.", clientDescriptor);

  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel<EhcacheEntityMessage> syncChannel, int concurrencyKey) {
    throw new UnsupportedOperationException("Active/passive is not supported yet");
  }

  @Override
  public void createNew() {
    //nothing to do
  }

  @Override
  public void loadExisting() {
    //nothing to do
  }

  private void validateClientConnected(ClientDescriptor clientDescriptor) throws ClusterException {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the Clustered Tier Manager");
    }
  }

  private void validateClientAttached(ClientDescriptor clientDescriptor) throws ClusterException {
    validateClientConnected(clientDescriptor);
    if (!clientStateMap.get(clientDescriptor).isAttached()) {
      throw new LifecycleException("Client " + clientDescriptor + " is not attached to the Clustered Tier Manager");
    }
  }

  private void validateClusteredTierManagerConfigured(ClientDescriptor clientDescriptor) throws ClusterException {
    validateClientAttached(clientDescriptor);
    if (!ehcacheStateService.isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }
  }

  private EhcacheEntityResponse invokeLifeCycleOperation(ClientDescriptor clientDescriptor, LifecycleMessage message) throws ClusterException {
    switch (message.operation()) {
      case CONFIGURE:
        configure(clientDescriptor, (ConfigureStoreManager) message);
        break;
      case VALIDATE:
        validate(clientDescriptor, (ValidateStoreManager) message);
        break;
      case CREATE_SERVER_STORE:
        createServerStore(clientDescriptor, (CreateServerStore) message);
        break;
      case VALIDATE_SERVER_STORE:
        validateServerStore(clientDescriptor, (ValidateServerStore) message);
        break;
      case RELEASE_SERVER_STORE:
        releaseServerStore(clientDescriptor, (ReleaseServerStore) message);
        break;
      case DESTROY_SERVER_STORE:
        destroyServerStore(clientDescriptor, (DestroyServerStore) message);
        break;
      default:
        throw new IllegalMessageException("Unknown LifeCycle operation " + message);
    }
    return responseFactory.success();
  }

  private EhcacheEntityResponse invokeServerStoreOperation(ClientDescriptor clientDescriptor, ServerStoreOpMessage message) throws ClusterException {
    validateClusteredTierManagerConfigured(clientDescriptor);

    ServerStoreImpl cacheStore = ehcacheStateService.getStore(message.getCacheId());
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("Clustered tier does not exist : '" + message.getCacheId() + "'");
    }

    if (!storeClientMap.get(message.getCacheId()).contains(clientDescriptor)) {
      // An operation on a store should never happen from client no attached onto that store
      throw new LifecycleException("Client not attached to clustered tier '" + message.getCacheId() + "'");
    }

    switch (message.operation()) {
      case GET: {
        ServerStoreOpMessage.GetMessage getMessage = (ServerStoreOpMessage.GetMessage) message;
        return responseFactory.response(cacheStore.get(getMessage.getKey()));
      }
      case APPEND: {
        ServerStoreOpMessage.AppendMessage appendMessage = (ServerStoreOpMessage.AppendMessage)message;
        cacheStore.append(appendMessage.getKey(), appendMessage.getPayload());
        invalidateHashForClient(clientDescriptor, appendMessage.getCacheId(), appendMessage.getKey());
        return responseFactory.success();
      }
      case GET_AND_APPEND: {
        ServerStoreOpMessage.GetAndAppendMessage getAndAppendMessage = (ServerStoreOpMessage.GetAndAppendMessage)message;
        EhcacheEntityResponse response = responseFactory.response(cacheStore.getAndAppend(getAndAppendMessage.getKey(), getAndAppendMessage.getPayload()));
        invalidateHashForClient(clientDescriptor, getAndAppendMessage.getCacheId(), getAndAppendMessage.getKey());
        return response;
      }
      case REPLACE: {
        ServerStoreOpMessage.ReplaceAtHeadMessage replaceAtHeadMessage = (ServerStoreOpMessage.ReplaceAtHeadMessage) message;
        cacheStore.replaceAtHead(replaceAtHeadMessage.getKey(), replaceAtHeadMessage.getExpect(), replaceAtHeadMessage.getUpdate());
        return responseFactory.success();
      }
      case CLIENT_INVALIDATION_ACK: {
        ServerStoreOpMessage.ClientInvalidationAck clientInvalidationAck = (ServerStoreOpMessage.ClientInvalidationAck) message;
        String cacheId = message.getCacheId();
        int invalidationId = clientInvalidationAck.getInvalidationId();
        LOGGER.debug("SERVER: got notification of invalidation ack in cache {} from {} (ID {})", cacheId, clientDescriptor, invalidationId);
        clientInvalidated(clientDescriptor, invalidationId);
        return responseFactory.success();
      }
      case CLEAR: {
        String cacheId = message.getCacheId();
        cacheStore.clear();
        invalidateAll(clientDescriptor, cacheId);
        return responseFactory.success();
      }
      default:
        throw new IllegalMessageException("Unknown ServerStore operation : " + message);
    }
  }

  private EhcacheEntityResponse invokeStateRepositoryOperation(ClientDescriptor clientDescriptor, StateRepositoryOpMessage message) throws ClusterException {
    validateClusteredTierManagerConfigured(clientDescriptor);
    return ehcacheStateService.getStateRepositoryManager().invoke(message);
  }

  private void invalidateHashAfterEviction(String cacheId, long key) {
    Set<ClientDescriptor> clientsToInvalidate = Collections.newSetFromMap(new ConcurrentHashMap<ClientDescriptor, Boolean>());
    clientsToInvalidate.addAll(storeClientMap.get(cacheId));

    for (ClientDescriptor clientDescriptorThatHasToInvalidate : clientsToInvalidate) {
      LOGGER.debug("SERVER: eviction happened; asking client {} to invalidate hash {} from cache {}", clientDescriptorThatHasToInvalidate, key, cacheId);
      try {
        clientCommunicator.sendNoResponse(clientDescriptorThatHasToInvalidate, serverInvalidateHash(cacheId, key));
      } catch (MessageCodecException mce) {
        //TODO: what should be done here?
        LOGGER.error("Codec error", mce);
      }
    }
  }

  private void invalidateHashForClient(ClientDescriptor originatingClientDescriptor, String cacheId, long key) {
    int invalidationId = invalidationIdGenerator.getAndIncrement();
    Set<ClientDescriptor> clientsToInvalidate = Collections.newSetFromMap(new ConcurrentHashMap<ClientDescriptor, Boolean>());
    clientsToInvalidate.addAll(storeClientMap.get(cacheId));
    clientsToInvalidate.remove(originatingClientDescriptor);

    InvalidationHolder invalidationHolder = null;
    if (ehcacheStateService.getStore(cacheId).getStoreConfiguration().getConsistency() == Consistency.STRONG) {
      invalidationHolder = new InvalidationHolder(originatingClientDescriptor, clientsToInvalidate, cacheId, key);
      clientsWaitingForInvalidation.put(invalidationId, invalidationHolder);
    }

    LOGGER.debug("SERVER: requesting {} client(s) invalidation of hash {} in cache {} (ID {})", clientsToInvalidate.size(), key, cacheId, invalidationId);
    for (ClientDescriptor clientDescriptorThatHasToInvalidate : clientsToInvalidate) {
      LOGGER.debug("SERVER: asking client {} to invalidate hash {} from cache {} (ID {})", clientDescriptorThatHasToInvalidate, key, cacheId, invalidationId);
      try {
        clientCommunicator.sendNoResponse(clientDescriptorThatHasToInvalidate, clientInvalidateHash(cacheId, key, invalidationId));
      } catch (MessageCodecException mce) {
        //TODO: what should be done here?
        LOGGER.error("Codec error", mce);
      }
    }

    if (invalidationHolder != null && clientsToInvalidate.isEmpty()) {
      clientInvalidated(invalidationHolder.clientDescriptorWaitingForInvalidation, invalidationId);
    }
  }

  private void invalidateAll(ClientDescriptor originatingClientDescriptor, String cacheId) {
    int invalidationId = invalidationIdGenerator.getAndIncrement();
    Set<ClientDescriptor> clientsToInvalidate = Collections.newSetFromMap(new ConcurrentHashMap<ClientDescriptor, Boolean>());
    clientsToInvalidate.addAll(storeClientMap.get(cacheId));
    clientsToInvalidate.remove(originatingClientDescriptor);

    InvalidationHolder invalidationHolder = null;
    if (ehcacheStateService.getStore(cacheId).getStoreConfiguration().getConsistency() == Consistency.STRONG) {
      invalidationHolder = new InvalidationHolder(originatingClientDescriptor, clientsToInvalidate, cacheId);
      clientsWaitingForInvalidation.put(invalidationId, invalidationHolder);
    }

    LOGGER.debug("SERVER: requesting {} client(s) invalidation of all in cache {} (ID {})", clientsToInvalidate.size(), cacheId, invalidationId);
    for (ClientDescriptor clientDescriptorThatHasToInvalidate : clientsToInvalidate) {
      LOGGER.debug("SERVER: asking client {} to invalidate all from cache {} (ID {})", clientDescriptorThatHasToInvalidate, cacheId, invalidationId);
      try {
        clientCommunicator.sendNoResponse(clientDescriptorThatHasToInvalidate, clientInvalidateAll(cacheId, invalidationId));
      } catch (MessageCodecException mce) {
        //TODO: what should be done here?
        LOGGER.error("Codec error", mce);
      }
    }

    if (invalidationHolder != null && clientsToInvalidate.isEmpty()) {
      clientInvalidated(invalidationHolder.clientDescriptorWaitingForInvalidation, invalidationId);
    }
  }

  private void clientInvalidated(ClientDescriptor clientDescriptor, int invalidationId) {
    InvalidationHolder invalidationHolder = clientsWaitingForInvalidation.get(invalidationId);

    if (ehcacheStateService.getStore(invalidationHolder.cacheId).getStoreConfiguration().getConsistency() == Consistency.STRONG) {
      invalidationHolder.clientsHavingToInvalidate.remove(clientDescriptor);
      if (invalidationHolder.clientsHavingToInvalidate.isEmpty()) {
        if (clientsWaitingForInvalidation.remove(invalidationId) != null) {
          try {
            Long key = invalidationHolder.key;
            if (key == null) {
              clientCommunicator.sendNoResponse(invalidationHolder.clientDescriptorWaitingForInvalidation, allInvalidationDone(invalidationHolder.cacheId));
              LOGGER.debug("SERVER: notifying originating client that all other clients invalidated all in cache {} from {} (ID {})", invalidationHolder.cacheId, clientDescriptor, invalidationId);
            } else {
              clientCommunicator.sendNoResponse(invalidationHolder.clientDescriptorWaitingForInvalidation, hashInvalidationDone(invalidationHolder.cacheId, key));
              LOGGER.debug("SERVER: notifying originating client that all other clients invalidated key {} in cache {} from {} (ID {})", key, invalidationHolder.cacheId, clientDescriptor, invalidationId);
            }
          } catch (MessageCodecException mce) {
            //TODO: what should be done here?
            LOGGER.error("Codec error", mce);
          }
        }
      }
    }
  }

  /**
   * {@inheritDoc}
   * <p>
   *   This method is invoked in response to a call to a {@code com.tc.objectserver.api.ServerEntityRequest}
   *   message for a {@code ServerEntityAction.DESTROY_ENTITY} request which is sent via a call to the
   *   {@code ClusteringService.destroyAll} method.  This method is expected to be called only when no
   *   clients actively using this entity.
   * </p>
   */
  @Override
  public void destroy() {

    /*
     * Ensure the allocated stores are closed out.
     */
    for (String store : ehcacheStateService.getStores()) {
      final Set<ClientDescriptor> attachedClients = storeClientMap.get(store);
      if (attachedClients != null && !attachedClients.isEmpty()) {
        // This is logically an AssertionError; logging and continuing destroy
        LOGGER.error("Clustered tier '{}' has {} clients attached during clustered tier manager destroy", store);
      }

      LOGGER.info("Destroying clustered tier '{}' for clustered tier manager destroy", store);
      storeClientMap.remove(store);
    }

    ehcacheStateService.destroy();

  }

  /**
   * Handles the {@link LifecycleMessage.ConfigureStoreManager ConfigureStoreManager} message.  This method creates the shared
   * resource pools to be available to clients of this {@code EhcacheActiveEntity}.
   *
   * @param clientDescriptor the client identifier requesting store manager configuration
   * @param message the {@code ConfigureStoreManager} message carrying the desired shared resource pool configuration
   */
  private void configure(ClientDescriptor clientDescriptor, ConfigureStoreManager message) throws ClusterException {
    validateClientConnected(clientDescriptor);
    ehcacheStateService.configure(message);
    this.clientStateMap.get(clientDescriptor).attach();
  }

  /**
   * Handles the {@link ValidateStoreManager ValidateStoreManager} message.  This message is used by a client to
   * connect to an established {@code EhcacheActiveEntity}.  This method validates the client-provided configuration
   * against the existing configuration to ensure compatibility.
   *
   * @param clientDescriptor the client identifier requesting attachment to a configured store manager
   * @param message the {@code ValidateStoreManager} message carrying the client expected resource pool configuration
   */
  private void validate(ClientDescriptor clientDescriptor, ValidateStoreManager message) throws ClusterException {
    validateClientConnected(clientDescriptor);
    ehcacheStateService.validate(message);
    this.clientStateMap.get(clientDescriptor).attach();
  }

  /**
   * Handles the {@link CreateServerStore CreateServerStore} message.  This message is used by a client to
   * create a new {@link ServerStore}; if the {@code ServerStore} exists, a failure is returned to the client.
   * Once created, the client is registered with the {@code ServerStore}.  The registration persists until
   * the client disconnects or explicitly releases the store.
   * <p>
   *   If the store uses a dedicated resource, this method allocates a new dedicated resource pool associated
   *   with the cache identifier/name.
   * </p>
   * <p>
   *   Once created, a {@code ServerStore} persists until explicitly destroyed by a
   *   {@link DestroyServerStore DestroyServerStore} message or this {@code EhcacheActiveEntity} is
   *   destroyed through the {@link #destroy()} method.
   * </p>
   *
   * @param clientDescriptor the client identifier requesting store creation
   * @param createServerStore the {@code CreateServerStore} message carrying the desire store configuration
   */
  private void createServerStore(ClientDescriptor clientDescriptor, CreateServerStore createServerStore) throws ClusterException {
    validateClusteredTierManagerConfigured(clientDescriptor);
    if(createServerStore.getStoreConfiguration().getPoolAllocation() instanceof PoolAllocation.Unknown) {
      throw new LifecycleException("Clustered tier can't be created with an Unknown resource pool");
    }

    final String name = createServerStore.getName();    // client cache identifier/name

    LOGGER.info("Client {} creating new clustered tier '{}'", clientDescriptor, name);

    ServerStoreConfiguration storeConfiguration = createServerStore.getStoreConfiguration();

    ServerStoreImpl serverStore = ehcacheStateService.createStore(name, storeConfiguration);
    serverStore.setEvictionListener(new ServerStoreEvictionListener() {
      @Override
      public void onEviction(long key) {
        invalidateHashAfterEviction(name, key);
      }
    });
    attachStore(clientDescriptor, name);
  }

  /**
   * Handles the {@link ValidateServerStore ValidateServerStore} message.  This message is used by a client to
   * attach to an existing {@link ServerStore}; if the {@code ServerStore} does not exist, a failure is returned
   * to the client.  The client is registered with the {@code ServerStore}.  The registration persists until the
   * client disconnects or explicitly releases the store.  This method performs a compatibility check between the
   * store configuration desired by the client and the existing store configuration; if the configurations are not
   * compatible, a failure is returned to the client.
   *
   * @param clientDescriptor the client identifier requesting attachment to an existing store
   * @param validateServerStore the {@code ValidateServerStore} message carrying the desired store configuration
   */
  private void validateServerStore(ClientDescriptor clientDescriptor, ValidateServerStore validateServerStore) throws ClusterException {
    validateClusteredTierManagerConfigured(clientDescriptor);

    String name = validateServerStore.getName();
    ServerStoreConfiguration clientConfiguration = validateServerStore.getStoreConfiguration();

    LOGGER.info("Client {} validating clustered tier '{}'", clientDescriptor, name);
    ServerStoreImpl store = ehcacheStateService.getStore(name);
    if (store != null) {
      storeCompatibility.verify(store.getStoreConfiguration(), clientConfiguration);
      attachStore(clientDescriptor, name);
    } else {
      throw new InvalidStoreException("Clustered tier '" + name + "' does not exist");
    }
  }

  /**
   * Handles the {@link ReleaseServerStore ReleaseServerStore} message.  This message is used by a client to
   * explicitly release its attachment to a {@code ServerStore}.  If the client is not registered with the store,
   * a failure is returned to the client.
   *
   * @param clientDescriptor the client identifier requesting store release
   * @param releaseServerStore the {@code ReleaseServerStore} message identifying the store to release
   */
  private void releaseServerStore(ClientDescriptor clientDescriptor, ReleaseServerStore releaseServerStore) throws ClusterException {
    validateClusteredTierManagerConfigured(clientDescriptor);

    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    String name = releaseServerStore.getName();

    LOGGER.info("Client {} releasing clustered tier '{}'", clientDescriptor, name);
    ServerStoreImpl store = ehcacheStateService.getStore(name);
    if (store != null) {

      boolean removedFromClient = clientState.removeStore(name);
      removedFromClient |= detachStore(clientDescriptor, name);

      if (!removedFromClient) {
        throw new InvalidStoreException("Clustered tier '" + name + "' is not in use by client");
      }
    } else {
      throw new InvalidStoreException("Clustered tier '" + name + "' does not exist");
    }
  }

  /**
   * Handles the {@link DestroyServerStore DestroyServerStore} message.  This message is used by a client to
   * explicitly destroy a {@link ServerStore} instance.  If the {@code ServerStore} does not exist or a client
   * is attached to the store, a failure is returned to the client.
   *
   * @param clientDescriptor the client identifier requesting store destruction
   * @param destroyServerStore the {@code DestroyServerStore} message identifying the store to release
   */
  private void destroyServerStore(ClientDescriptor clientDescriptor, DestroyServerStore destroyServerStore) throws ClusterException {
    validateClusteredTierManagerConfigured(clientDescriptor);

    String name = destroyServerStore.getName();
    final Set<ClientDescriptor> clients = storeClientMap.get(name);
    if (clients != null && !clients.isEmpty()) {
      throw new ResourceBusyException("Cannot destroy clustered tier '" + name + "': in use by " + clients.size() + " other client(s)");
    }

    LOGGER.info("Client {} destroying clustered tier '{}'", clientDescriptor, name);
    ehcacheStateService.destroyServerStore(name);
    storeClientMap.remove(name);
  }

  /**
   * Establishes a registration of a client against a store.
   * <p>
   *   Once registered, a client is associated with a store until the client disconnects or
   *   explicitly releases the store.
   * </p>
   *
   * @param clientDescriptor the client to connect
   * @param storeId the store id to which the client is connecting
   */
  private void attachStore(ClientDescriptor clientDescriptor, String storeId) {
    boolean updated = false;
    while (!updated) {
      Set<ClientDescriptor> clients = storeClientMap.get(storeId);
      Set<ClientDescriptor> newClients;
      if (clients == null) {
        newClients = new HashSet<ClientDescriptor>();
        newClients.add(clientDescriptor);
        updated = (storeClientMap.putIfAbsent(storeId, newClients) == null);

      } else if (!clients.contains(clientDescriptor)) {
        newClients = new HashSet<ClientDescriptor>(clients);
        newClients.add(clientDescriptor);
        updated = storeClientMap.replace(storeId, clients, newClients);

      } else {
        // Already registered
        updated = true;
      }
    }

    final ClientState clientState = clientStateMap.get(clientDescriptor);
    clientState.addStore(storeId);

    LOGGER.info("Client {} attached to clustered tier '{}'", clientDescriptor, storeId);
  }

  /**
   * Removes the registration of a client against a store.
   *
   * @param clientDescriptor the client to disconnect
   * @param storeId the store id from which the client is disconnecting
   *
   * @return {@code true} if the client had been registered and was removed; {@code false} if
   *        the client was not registered against the store
   */
  private boolean detachStore(ClientDescriptor clientDescriptor, String storeId) {
    boolean wasRegistered = false;

    boolean updated = false;
    while (!updated) {
      Set<ClientDescriptor> clients = storeClientMap.get(storeId);
      if (clients != null && clients.contains(clientDescriptor)) {
        wasRegistered = true;
        Set<ClientDescriptor> newClients = new HashSet<ClientDescriptor>(clients);
        newClients.remove(clientDescriptor);
        updated = storeClientMap.replace(storeId, clients, newClients);
      } else {
        updated = true;
      }
    }

    if (wasRegistered) {
      LOGGER.info("Client {} detached from clustered tier '{}'", clientDescriptor, storeId);
    }

    return wasRegistered;
  }

  ConcurrentMap<Integer, InvalidationHolder> getClientsWaitingForInvalidation() {
    return clientsWaitingForInvalidation;
  }

  /**
   * Represents a client's state against an {@link EhcacheActiveEntity}.
   */
  private static class ClientState {
    /**
     * Indicates if the client has either configured or validated with clustered store manager.
     */
    private boolean attached = false;

    /**
     * The set of stores to which the client has attached.
     */
    private final Set<String> attachedStores = new HashSet<String>();

    boolean isAttached() {
      return attached;
    }

    void attach() {
      this.attached = true;
    }

    boolean addStore(String storeName) {
      return this.attachedStores.add(storeName);
    }

    boolean removeStore(String storeName) {
      return this.attachedStores.remove(storeName);
    }

    Set<String> getAttachedStores() {
      return Collections.unmodifiableSet(new HashSet<String>(this.attachedStores));
    }
  }

}
