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
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.clustered.common.internal.exceptions.ClusteredEhcacheException;
import org.ehcache.clustered.common.internal.exceptions.IllegalMessageException;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerSideConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreException;
import org.ehcache.clustered.common.internal.exceptions.InvalidStoreManagerException;
import org.ehcache.clustered.common.internal.exceptions.LifecycleException;
import org.ehcache.clustered.common.internal.exceptions.ResourceBusyException;
import org.ehcache.clustered.common.internal.exceptions.ResourceConfigurationException;
import org.ehcache.clustered.common.internal.exceptions.ServerMisconfigurationException;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse;

import org.ehcache.clustered.common.internal.messages.EhcacheEntityResponseFactory;
import org.ehcache.clustered.common.internal.messages.LifecycleMessage;
import org.ehcache.clustered.common.internal.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.internal.store.ServerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientCommunicator;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.MessageCodecException;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceConfiguration;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapresource.OffHeapResources;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.allInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateAll;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.clientInvalidateHash;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.hashInvalidationDone;
import static org.ehcache.clustered.common.internal.messages.EhcacheEntityResponse.serverInvalidateHash;
import static org.terracotta.offheapstore.util.MemoryUnit.GIGABYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

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
  private final ServiceRegistry services;
  private final Set<String> offHeapResourceIdentifiers;

  /**
   * The name of the resource to use for dedicated resource pools not identifying a resource from which
   * space for the pool is obtained.  This value may be {@code null};
   */
  private String defaultServerResource;

  /**
   * The clustered shared resource pools specified by the CacheManager creating this {@code EhcacheActiveEntity}.
   * The index is the name assigned to the shared resource pool in the cache manager configuration.
   */
  private Map<String, ResourcePageSource> sharedResourcePools;

  /**
   * The clustered dedicated resource pools specified by caches defined in CacheManagers using this
   * {@code EhcacheActiveEntity}.  The index is the cache identifier (alias).
   */
  private Map<String, ResourcePageSource> dedicatedResourcePools = new HashMap<String, ResourcePageSource>();

  /**
   * The clustered stores representing the server-side of a {@code ClusterStore}.
   * The index is the cache alias/identifier.
   */
  private Map<String, ServerStoreImpl> stores = Collections.emptyMap();

  /**
   * Tracks the state of a connected client.  An entry is added to this map when the
   * {@link #connected(ClientDescriptor)} method is invoked for a client and removed when the
   * {@link #disconnected(ClientDescriptor)} method is invoked for the client.
   */
  private final Map<ClientDescriptor, ClientState> clientStateMap = new HashMap<ClientDescriptor, ClientState>();

  private final ConcurrentHashMap<String, Set<ClientDescriptor>> storeClientMap =
      new ConcurrentHashMap<String, Set<ClientDescriptor>>();

  private final ServerStoreCompatibility storeCompatibility = new ServerStoreCompatibility();
  private final EhcacheEntityResponseFactory responseFactory;
  private final ConcurrentMap<Integer, InvalidationHolder> clientsWaitingForInvalidation = new ConcurrentHashMap<Integer, InvalidationHolder>();
  private final AtomicInteger invalidationIdGenerator = new AtomicInteger();
  private final ClientCommunicator clientCommunicator;

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
    this.services = services;
    this.responseFactory = new EhcacheEntityResponseFactory();
    this.clientCommunicator = services.getService(new CommunicatorServiceConfiguration());
    OffHeapResources offHeapResources = services.getService(new OffHeapResourcesServiceConfiguration());
    if (offHeapResources == null) {
      this.offHeapResourceIdentifiers = Collections.emptySet();
    } else {
      this.offHeapResourceIdentifiers = offHeapResources.getAllIdentifiers();
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
   * Gets the set of {@code ServerStore} identifiers defined in this {@code EhcacheActiveEntity}.
   *
   * @return an unmodifiable copy of the store ids
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  Set<String> getStores() {
    return Collections.unmodifiableSet(new HashSet<String>(stores.keySet()));
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

  /**
   * Gets the name of the default server resource.
   *
   * @return the name of the default server resource; may be {@code null} is none is defined
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  String getDefaultServerResource() {
    return defaultServerResource;
  }

  /**
   * Gets the set of defined shared resource pools.
   *
   * @return an unmodifiable set of resource pool identifiers
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  Set<String> getSharedResourcePoolIds() {
    return (sharedResourcePools == null
        ? Collections.<String>emptySet()
        : Collections.unmodifiableSet(new HashSet<String>(sharedResourcePools.keySet())));
  }

  /**
   * Gets the set of defined dedicated resource pools.
   *
   * @return an unmodifiable set of resource pool identifiers
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  Set<String> getDedicatedResourcePoolIds() {
    return Collections.unmodifiableSet(new HashSet<String>(dedicatedResourcePools.keySet()));
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
      if (this.offHeapResourceIdentifiers.isEmpty()) {
        throw new ServerMisconfigurationException("Server started without any offheap resources defined." +
                                                  " Check your server configuration and define at least one offheap resource.");
      }

      switch (message.getType()) {
        case LIFECYCLE_OP:
          return invokeLifeCycleOperation(clientDescriptor, (LifecycleMessage) message);
        case SERVER_STORE_OP:
          return invokeServerStoreOperation(clientDescriptor, (ServerStoreOpMessage) message);
        default:
          throw new IllegalMessageException("Unknown message : " + message);
      }
    } catch (ClusteredEhcacheException e) {
      return responseFactory.failure(e);
    } catch (Exception e) {
      LOGGER.error("Unexpected exception raised during operation: " + message, e);
      return responseFactory.failure(e);
    }
  }

  @Override
  public void handleReconnect(ClientDescriptor clientDescriptor, byte[] extendedReconnectData) {
    //nothing to do
  }

  @Override
  public void synchronizeKeyToPassive(PassiveSynchronizationChannel syncChannel, int concurrencyKey) {
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

  private EhcacheEntityResponse invokeLifeCycleOperation(ClientDescriptor clientDescriptor, LifecycleMessage message) throws ClusteredEhcacheException {
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

  private EhcacheEntityResponse invokeServerStoreOperation(ClientDescriptor clientDescriptor, ServerStoreOpMessage message) throws ClusteredEhcacheException {
    ServerStore cacheStore = stores.get(message.getCacheId());
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      throw new LifecycleException("Clustered tier does not exist : '" + message.getCacheId() + "'");
    }

    if (!clientStateMap.get(clientDescriptor).isAttached()) {
      // An operation on a store should never happen from an unattached client
      throw new LifecycleException("Client not attached");
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
    if (stores.get(cacheId).getStoreConfiguration().getConsistency() == Consistency.STRONG) {
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
    if (stores.get(cacheId).getStoreConfiguration().getConsistency() == Consistency.STRONG) {
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

    if (stores.get(invalidationHolder.cacheId).getStoreConfiguration().getConsistency() == Consistency.STRONG) {
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

    defaultServerResource = null;

    /*
     * Ensure the allocated stores are closed out.
     */
    final Iterator<Entry<String, ServerStoreImpl>> storeIterator = stores.entrySet().iterator();
    while (storeIterator.hasNext()) {
      Entry<String, ServerStoreImpl> storeEntry = storeIterator.next();
      final Set<ClientDescriptor> attachedClients = storeClientMap.get(storeEntry.getKey());
      if (attachedClients != null && !attachedClients.isEmpty()) {
        // This is logically an AssertionError; logging and continuing destroy
        LOGGER.error("Clustered tier '{}' has {} clients attached during clustered tier manager destroy", storeEntry.getKey());
      }

      LOGGER.info("Destroying clustered tier '{}' for clustered tier manager destroy", storeEntry.getKey());
      // TODO: ServerStore closure here ...
      storeClientMap.remove(storeEntry.getKey());
      storeIterator.remove();
    }

    /*
     * Remove the reservation for resource pool memory of resource pools.
     */
    releasePools("shared", this.sharedResourcePools);
    releasePools("dedicated", this.dedicatedResourcePools);

    this.sharedResourcePools = null;
  }

  /**
   * Handles the {@link LifecycleMessage.ConfigureStoreManager ConfigureStoreManager} message.  This method creates the shared
   * resource pools to be available to clients of this {@code EhcacheActiveEntity}.
   *
   * @param clientDescriptor the client identifier requesting store manager configuration
   * @param message the {@code ConfigureStoreManager} message carrying the desired shared resource pool configuration
   */
  private void configure(ClientDescriptor clientDescriptor, ConfigureStoreManager message) throws ClusteredEhcacheException {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the Clustered Tier Manager");
    }
    if (!isConfigured()) {
      LOGGER.info("Configuring server-side clustered tier manager");
      ServerSideConfiguration configuration = message.getConfiguration();

      this.defaultServerResource = configuration.getDefaultServerResource();
      if (this.defaultServerResource != null) {
        if (!offHeapResourceIdentifiers.contains(this.defaultServerResource)) {
          throw new ResourceConfigurationException("Default server resource '" + this.defaultServerResource
              + "' is not defined. Available resources are: " + offHeapResourceIdentifiers);
        }
      }

      this.sharedResourcePools = createPools(resolveResourcePools(configuration));
      this.stores = new HashMap<String, ServerStoreImpl>();

      clientState.attach();
    } else {
      throw new InvalidStoreManagerException("Clustered Tier Manager already configured");
    }
  }

  private boolean isConfigured() {
    return (sharedResourcePools != null);
  }

  /**
   * Handles the {@link ValidateStoreManager ValidateStoreManager} message.  This message is used by a client to
   * connect to an established {@code EhcacheActiveEntity}.  This method validates the client-provided configuration
   * against the existing configuration to ensure compatibility.
   *
   * @param clientDescriptor the client identifier requesting attachment to a configured store manager
   * @param message the {@code ValidateStoreManager} message carrying the client expected resource pool configuration
   */
  private void validate(ClientDescriptor clientDescriptor, ValidateStoreManager message) throws ClusteredEhcacheException {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the Clustered Tier Manager");
    }
    if (!isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }
    ServerSideConfiguration incomingConfig = message.getConfiguration();

    if(incomingConfig != null) {
      checkConfigurationCompatibility(incomingConfig);
    }
    clientState.attach();
  }

  /**
   * Checks whether the {@link ServerSideConfiguration} sent from the client is equal with the ServerSideConfiguration
   * that is already configured on the server.
   * @param incomingConfig the ServerSideConfiguration to be validated.  This is sent from a client
   * @throws IllegalArgumentException if configurations do not match
   */
  private void checkConfigurationCompatibility(ServerSideConfiguration incomingConfig) throws InvalidServerSideConfigurationException {
    if (!nullSafeEquals(this.defaultServerResource, incomingConfig.getDefaultServerResource())) {
      throw new InvalidServerSideConfigurationException("Default resource not aligned. "
              + "Client: " + incomingConfig.getDefaultServerResource() + " "
              + "Server: " + defaultServerResource);
    } else if(!sharedResourcePools.keySet().equals(incomingConfig.getResourcePools().keySet())) {
      throw new InvalidServerSideConfigurationException("Pool names not equal. "
              + "Client: " + incomingConfig.getResourcePools().keySet() + " "
              + "Server: " + sharedResourcePools.keySet().toString());
    }

    for(Entry<String, Pool> pool : resolveResourcePools(incomingConfig).entrySet()) {
      Pool serverPool = this.sharedResourcePools.get(pool.getKey()).getPool();

      if(!serverPool.equals(pool.getValue())) {
        throw new InvalidServerSideConfigurationException("Pool '" + pool.getKey() + "' not equal. "
                + "Client: " + pool.getValue() + " "
                + "Server: " + serverPool);
      }
    }
  }

  private static boolean nullSafeEquals(Object s1, Object s2) {
    return (s1 == null ? s2 == null : s1.equals(s2));
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
  private void createServerStore(ClientDescriptor clientDescriptor, CreateServerStore createServerStore) throws ClusteredEhcacheException {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the Clustered Tier Manager");
    }
    if (!clientState.isAttached()) {
      throw new LifecycleException("Client " + clientDescriptor + " is not attached to the Clustered Tier Manager");
    }
    if (!isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }
    if(createServerStore.getStoreConfiguration().getPoolAllocation() instanceof PoolAllocation.Unknown) {
      throw new LifecycleException("Clustered tier can't be created with an Unknown resource pool");
    }

    final String name = createServerStore.getName();    // client cache identifier/name

    LOGGER.info("Client {} creating new clustered tier '{}'", clientDescriptor, name);

    if (stores.containsKey(name)) {
      throw new InvalidStoreException("Clustered tier '" + name + "' already exists");
    }

    ServerStoreConfiguration storeConfiguration = createServerStore.getStoreConfiguration();
    ResourcePageSource resourcePageSource;
    PoolAllocation allocation = storeConfiguration.getPoolAllocation();
    if (allocation instanceof PoolAllocation.Dedicated) {
      /*
       * Dedicated allocation pools are taken directly from a specified resource, not a shared pool, and
       * identified by the cache identifier/name.
       */
//<<<<<<< HEAD
      if (dedicatedResourcePools.containsKey(name)) {
        throw new ResourceConfigurationException("Fixed resource pool for clustered tier '" + name + "' already exists");
      } else {
        PoolAllocation.Dedicated dedicatedAllocation = (PoolAllocation.Dedicated)allocation;
        String resourceName = dedicatedAllocation.getResourceName();
        if (resourceName == null) {
          if (defaultServerResource == null) {
            throw new ResourceConfigurationException("Fixed pool for clustered tier '" + name + "' not defined; default server resource not configured");
          } else {
            resourceName = defaultServerResource;
          }
        }
        resourcePageSource = createPageSource(name, new Pool(dedicatedAllocation.getSize(), resourceName));
        dedicatedResourcePools.put(name, resourcePageSource);
      }
    } else if (allocation instanceof PoolAllocation.Shared) {
      /*
       * Shared allocation pools are created during EhcacheActiveEntity configuration.
       */
      PoolAllocation.Shared sharedAllocation = (PoolAllocation.Shared)allocation;
      resourcePageSource = sharedResourcePools.get(sharedAllocation.getResourcePoolName());
      if(resourcePageSource == null) {
        throw new ResourceConfigurationException("Shared pool named '" + sharedAllocation.getResourcePoolName() + "' undefined.");
      }

    } else {
      throw new IllegalMessageException("Unexpected PoolAllocation type: " + allocation.getClass().getName());
    }

    ServerStoreImpl serverStore = new ServerStoreImpl(storeConfiguration, resourcePageSource);
    serverStore.setEvictionListener(new ServerStoreEvictionListener() {
      @Override
      public void onEviction(long key) {
        invalidateHashAfterEviction(name, key);
      }
    });
    stores.put(name, serverStore);
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
  private void validateServerStore(ClientDescriptor clientDescriptor, ValidateServerStore validateServerStore) throws ClusteredEhcacheException {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the Clustered Tier Manager");
    }
    if (!clientState.isAttached()) {
      throw new LifecycleException("Client " + clientDescriptor + " is not attached to the Clustered Tier Manager");
    }
    if (!isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }

    String name = validateServerStore.getName();
    ServerStoreConfiguration clientConfiguration = validateServerStore.getStoreConfiguration();

    LOGGER.info("Client {} validating clustered tier '{}'", clientDescriptor, name);
    ServerStoreImpl store = stores.get(name);
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
  private void releaseServerStore(ClientDescriptor clientDescriptor, ReleaseServerStore releaseServerStore) throws ClusteredEhcacheException {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the Clustered Tier Manager");
    }
    if (!clientState.isAttached()) {
      throw new LifecycleException("Client " + clientDescriptor + " is not attached to the Clustered Tier Manager");
    }
    if (!isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }

    String name = releaseServerStore.getName();

    LOGGER.info("Client {} releasing clustered tier '{}'", clientDescriptor, name);
    ServerStoreImpl store = stores.get(name);
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
  private void destroyServerStore(ClientDescriptor clientDescriptor, DestroyServerStore destroyServerStore) throws ClusteredEhcacheException {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      throw new LifecycleException("Client " + clientDescriptor + " is not connected to the Clustered Tier Manager");
    }
    if (!clientState.isAttached()) {
      throw new LifecycleException("Client " + clientDescriptor + " is not attached to the Clustered Tier Manager");
    }
    if (!isConfigured()) {
      throw new LifecycleException("Clustered Tier Manager is not configured");
    }

    String name = destroyServerStore.getName();

    final Set<ClientDescriptor> clients = storeClientMap.get(name);
    if (clients != null && !clients.isEmpty()) {
      throw new ResourceBusyException("Can not destroy clustered tier '" + name + "': in use by " + clients.size() + " clients");
    }

    LOGGER.info("Client {} destroying clustered tier '{}'", clientDescriptor, name);
    final ServerStoreImpl store = stores.remove(name);
    if (store == null) {
      throw new InvalidStoreException("Clustered tier '" + name + "' does not exist");
    } else {
      /*
       * A ServerStore using a dedicated resource pool is the only referent to that pool.  When such a
       * ServerStore is destroyed, the associated dedicated resource pool must also be discarded.
       */
      ResourcePageSource expectedPageSource = dedicatedResourcePools.get(name);
      if (expectedPageSource != null) {
        if (store.getPageSource() == expectedPageSource) {
          dedicatedResourcePools.remove(name);
          releasePool("dedicated", name, expectedPageSource);
        } else {
          LOGGER.error("Client {} attempting to destroy clustered tier '{}' with unmatched page source", clientDescriptor, name);
        }
      }

      // TODO: ServerStore closure here ...
      storeClientMap.remove(name);
    }
  }

  private static Map<String, Pool> resolveResourcePools(ServerSideConfiguration configuration) throws InvalidServerSideConfigurationException {
    Map<String, Pool> pools = new HashMap<String, Pool>();
    for (Map.Entry<String, Pool> e : configuration.getResourcePools().entrySet()) {
      Pool pool = e.getValue();
      if (pool.getServerResource() == null) {
        if (configuration.getDefaultServerResource() == null) {
          throw new InvalidServerSideConfigurationException("Pool '" + e.getKey() + "' has no defined server resource, and no default value was available");
        } else {
          pools.put(e.getKey(), new Pool(pool.getSize(), configuration.getDefaultServerResource()));
        }
      } else {
        pools.put(e.getKey(), pool);
      }
    }
    return Collections.unmodifiableMap(pools);
  }

  private Map<String, ResourcePageSource> createPools(Map<String, Pool> resourcePools) throws ResourceConfigurationException {
    Map<String, ResourcePageSource> pools = new HashMap<String, ResourcePageSource>();
    try {
      for (Entry<String, Pool> e : resourcePools.entrySet()) {
        pools.put(e.getKey(), createPageSource(e.getKey(), e.getValue()));
      }
    } catch (ResourceConfigurationException e) {
      /*
       * If we fail during pool creation, back out any pools successfully created during this call.
       */
      if (!pools.isEmpty()) {
        LOGGER.warn("Failed to create shared resource pools; reversing reservations", e);
        releasePools("shared", pools);
      }
      throw e;
    } catch (RuntimeException e) {
      /*
       * If we fail during pool creation, back out any pools successfully created during this call.
       */
      if (!pools.isEmpty()) {
        LOGGER.warn("Failed to create shared resource pools; reversing reservations", e);
        releasePools("shared", pools);
      }
      throw e;
    }
    return pools;
  }

  private ResourcePageSource createPageSource(String poolName, Pool pool) throws ResourceConfigurationException {
    ResourcePageSource pageSource;
    OffHeapResource source = services.getService(OffHeapResourceIdentifier.identifier(pool.getServerResource()));
    if (source == null) {
      throw new ResourceConfigurationException("Non-existent server side resource '" + pool.getServerResource() +
                                               "'. Available resources are: " + offHeapResourceIdentifiers);
    } else if (source.reserve(pool.getSize())) {
      try {
        pageSource = new ResourcePageSource(pool);
      } catch (RuntimeException t) {
        source.release(pool.getSize());
        throw new ResourceConfigurationException("Failure allocating pool " + pool, t);
      }
      LOGGER.info("Reserved {} bytes from resource '{}' for pool '{}'", pool.getSize(), pool.getServerResource(), poolName);
    } else {
      throw new ResourceConfigurationException("Insufficient defined resources to allocate pool " + poolName + "=" + pool);
    }
    return pageSource;
  }

  private void releasePools(String poolType, Map<String, ResourcePageSource> resourcePools) {
    if (resourcePools == null) {
      return;
    }
    final Iterator<Entry<String, ResourcePageSource>> dedicatedPoolIterator = resourcePools.entrySet().iterator();
    while (dedicatedPoolIterator.hasNext()) {
      Entry<String, ResourcePageSource> poolEntry = dedicatedPoolIterator.next();
      releasePool(poolType, poolEntry.getKey(), poolEntry.getValue());
      dedicatedPoolIterator.remove();
    }
  }

  private void releasePool(String poolType, String poolName, ResourcePageSource resourcePageSource) {
    Pool pool = resourcePageSource.getPool();
    OffHeapResource source = services.getService(OffHeapResourceIdentifier.identifier(pool.getServerResource()));
    if (source != null) {
      source.release(pool.getSize());
      LOGGER.info("Released {} bytes from resource '{}' for {} pool '{}'", pool.getSize(), pool.getServerResource(), poolType, poolName);
    }
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
      LOGGER.info("Client {} detached from clistered tier '{}'", clientDescriptor, storeId);
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

  /**
   * Pairs a {@link Pool} and an {@link UpfrontAllocatingPageSource} instance providing storage
   * for the pool.
   */
  private static class ResourcePageSource implements PageSource {
    /**
     * A description of the resource allocation underlying this {@code PageSource}.
     */
    private final Pool pool;
    private final UpfrontAllocatingPageSource delegatePageSource;

    private ResourcePageSource(Pool pool) {
      this.pool = pool;
      this.delegatePageSource = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), pool.getSize(), GIGABYTES.toBytes(1), MEGABYTES.toBytes(128));
    }

    private Pool getPool() {
      return pool;
    }

    @Override
    public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
      return delegatePageSource.allocate(size, thief, victim, owner);
    }

    @Override
    public void free(Page page) {
      delegatePageSource.free(page);
    }

    @Override
    public String toString() {
      final StringBuilder sb = new StringBuilder("ResourcePageSource{");
      sb.append("pool=").append(pool);
      sb.append(", delegatePageSource=").append(delegatePageSource);
      sb.append('}');
      return sb.toString();
    }
  }
}
