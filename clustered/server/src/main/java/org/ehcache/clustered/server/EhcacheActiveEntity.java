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

import org.ehcache.clustered.common.ClusteredStoreValidationException;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerStoreCompatibility;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.ServerStoreConfiguration.PoolAllocation;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;

import org.ehcache.clustered.common.messages.EhcacheEntityResponseFactory;
import org.ehcache.clustered.common.messages.LifecycleMessage;
import org.ehcache.clustered.common.messages.ServerStoreOpMessage;
import org.ehcache.clustered.common.store.ServerStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import static org.terracotta.offheapstore.util.MemoryUnit.GIGABYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

import static org.ehcache.clustered.common.messages.LifecycleMessage.ConfigureStoreManager;
import static org.ehcache.clustered.common.messages.LifecycleMessage.CreateServerStore;
import static org.ehcache.clustered.common.messages.LifecycleMessage.DestroyServerStore;
import static org.ehcache.clustered.common.messages.LifecycleMessage.ReleaseServerStore;
import static org.ehcache.clustered.common.messages.LifecycleMessage.ValidateServerStore;
import static org.ehcache.clustered.common.messages.LifecycleMessage.ValidateStoreManager;

// TODO: Provide some mechanism to report on storage utilization -- PageSource provides little visibility
// TODO: Ensure proper operations for concurrent requests
class EhcacheActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheActiveEntity.class);

  private final UUID identity;
  private final ServiceRegistry services;

  /**
   * The name of the resource to use for fixed resource pools not identifying a resource from which
   * space for the pool is obtained.  This value may be {@code null};
   */
  private String defaultServerResource;

  /**
   * The clustered shared resource pools specified by the CacheManager creating this {@code EhcacheActiveEntity}.
   * The index is the name assigned to the shared resource pool in the cache manager configuration.
   */
  private Map<String, ResourcePageSource> sharedResourcePools;

  /**
   * The clustered fixed resource pools specified by caches defined in CacheManagers using this
   * {@code EhcacheActiveEntity}.  The index is the cache identifier (alias).
   */
  private Map<String, ResourcePageSource> fixedResourcePools = new HashMap<String, ResourcePageSource>();

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

  EhcacheActiveEntity(ServiceRegistry services, byte[] config) {
    this.identity = ClusteredEhcacheIdentity.deserialize(config);
    this.services = services;
    this.responseFactory = new EhcacheEntityResponseFactory();
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
   * Gets the set of defined fixed resource pools.
   *
   * @return an unmodifiable set of resource pool identifiers
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  Set<String> getFixedResourcePoolIds() {
    return Collections.unmodifiableSet(new HashSet<String>(fixedResourcePools.keySet()));
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
    switch (message.getType()) {
      case LIFECYCLE_OP: return invokeLifeCycleOperation(clientDescriptor, (LifecycleMessage) message);
      case SERVER_STORE_OP: return invokeServerStoreOperation((ServerStoreOpMessage) message);
      default: throw new IllegalArgumentException("Unknown message " + message);
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

  private EhcacheEntityResponse invokeLifeCycleOperation(ClientDescriptor clientDescriptor, LifecycleMessage message) {
    try {
      switch (message.operation()) {
        case CONFIGURE: return configure(clientDescriptor, (ConfigureStoreManager) message);
        case VALIDATE: return validate(clientDescriptor, (ValidateStoreManager) message);
        case CREATE_SERVER_STORE: return createServerStore(clientDescriptor, (CreateServerStore) message);
        case VALIDATE_SERVER_STORE: return validateServerStore(clientDescriptor, (ValidateServerStore) message);
        case RELEASE_SERVER_STORE: return releaseServerStore(clientDescriptor, (ReleaseServerStore) message);
        case DESTROY_SERVER_STORE: return destroyServerStore(clientDescriptor, (DestroyServerStore) message);
        default:
          String msg = "Unknown LifeCycle operation " + message;
          IllegalArgumentException cause = new IllegalArgumentException(msg);
          LOGGER.error(msg, cause);
          return responseFactory.failure(cause);
      }
    } catch (Exception e) {
      LOGGER.error("Unexpected exception raised during LifeCycle operation: " + e, e);
      return responseFactory.failure(e);
    }
  }

  private EhcacheEntityResponse invokeServerStoreOperation(ServerStoreOpMessage message) {
    ServerStore cacheStore = stores.get(message.getCacheId());
    if (cacheStore == null) {
      // An operation on a non-existent store should never get out of the client
      String msg = "Server Store not present for cacheId :" + message.getCacheId();
      IllegalStateException cause = new IllegalStateException(msg);
      LOGGER.error(msg, cause);
      return responseFactory.failure(cause);
    }
    try {
      switch (message.operation()) {
        case GET: return responseFactory.response(cacheStore.get(message.getKey()));
        case APPEND: cacheStore.append(message.getKey(), ((ServerStoreOpMessage.AppendMessage)message).getPayload());
          return responseFactory.success();
        case GET_AND_APPEND: return responseFactory.response(cacheStore.getAndAppend(message.getKey(), ((ServerStoreOpMessage.GetAndAppendMessage)message).getPayload()));
        case REPLACE:
          ServerStoreOpMessage.ReplaceAtHeadMessage replaceAtHeadMessage = (ServerStoreOpMessage.ReplaceAtHeadMessage)message;
          cacheStore.replaceAtHead(replaceAtHeadMessage.getKey(), replaceAtHeadMessage.getExpect(), replaceAtHeadMessage.getUpdate());
          return responseFactory.success();
        case CLEAR:
          cacheStore.clear();
          return responseFactory.success();
        default:
          String msg = "Unknown Server Store operation " + message;
          IllegalArgumentException cause = new IllegalArgumentException(msg);
          LOGGER.error(msg, cause);
          return responseFactory.failure(cause);
      }
    } catch (Exception e) {
      return responseFactory.failure(e);
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
        LOGGER.error("Store '{}' has {} clients attached during entity destroy", storeEntry.getKey());
      }

      LOGGER.info("Destroying server-side store '{}' for entity destroy", storeEntry.getKey());
      // TODO: ServerStore closure here ...
      storeClientMap.remove(storeEntry.getKey());
      storeIterator.remove();
    }

    /*
     * Remove the reservation for resource pool memory of resource pools.
     */
    releasePools("shared", this.sharedResourcePools);
    releasePools("fixed", this.fixedResourcePools);

    this.sharedResourcePools = null;
  }

  /**
   * Handles the {@link LifecycleMessage.ConfigureStoreManager ConfigureStoreManager} message.  This method creates the shared
   * resource pools to be available to clients of this {@code EhcacheActiveEntity}.
   *
   * @param clientDescriptor the client identifier requesting store manager configuration
   * @param message the {@code ConfigureStoreManager} message carrying the desired shared resource pool configuration
   *
   * @return an {@code EhcacheEntityResponse} indicating the success or failure of the configuration
   */
  private EhcacheEntityResponse configure(ClientDescriptor clientDescriptor, ConfigureStoreManager message) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not connected to the Clustered Store Manager"));
    }
    if (!isConfigured()) {
      LOGGER.info("Configuring server-side clustered store manager");
      ServerSideConfiguration configuration = message.getConfiguration();

      this.defaultServerResource = configuration.getDefaultServerResource();
      if (this.defaultServerResource != null) {
        OffHeapResource source = services.getService(OffHeapResourceIdentifier.identifier(this.defaultServerResource));
        if (source == null) {
          return responseFactory.failure(new IllegalArgumentException("Default server resource '" + this.defaultServerResource
              + "' is not defined"));
        }
      }

      try {
        this.sharedResourcePools = createPools(configuration.getResourcePools());
      } catch (RuntimeException e) {
        return responseFactory.failure(e);
      }
      this.stores = new HashMap<String, ServerStoreImpl>();

      clientState.attach();
      return responseFactory.success();
    } else {
      return responseFactory.failure(new IllegalStateException("Clustered Store Manager already configured"));
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
   *
   * @return an {@code EhcacheEntityResponse} indicating the success or failure of the consistency check
   */
  private EhcacheEntityResponse validate(ClientDescriptor clientDescriptor, ValidateStoreManager message) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not connected to the Clustered Store Manager"));
    }
    if (!isConfigured()) {
      return responseFactory.failure(new IllegalStateException("Clustered Store Manager is not configured"));
    }
    ServerSideConfiguration serverSideConfiguration = message.getConfiguration();

    if(isInheritedConfig(serverSideConfiguration)) {
      clientState.attach();
      return responseFactory.success();
    }

    StringBuilder sb = new StringBuilder();

    if (!nullSafeEquals(this.defaultServerResource, serverSideConfiguration.getDefaultServerResource())) {
      return responseFactory.failure(new IllegalArgumentException("Default resource not aligned"));
    } else if(!sharedResourcePoolsEqual(serverSideConfiguration, sb)) {
      return responseFactory.failure(new IllegalArgumentException("SharedPoolResources aren't valid. " + sb.toString()));
    } else {
      clientState.attach();
      return responseFactory.success();
    }
  }

  /**
   * This function checks to see if the client sent an empty {@link ServerSideConfiguration} which will inherit
   * from an already configured ServerSideConfiguration.
   * @param serverSideConfiguration the ServerSideConfiguration sent by the client to be validated.
   * @return returns true if the ServerSideConfiguration is to be inherited, otherwise false
   */
  private boolean isInheritedConfig(ServerSideConfiguration serverSideConfiguration) {
    return this.defaultServerResource != null &&
            serverSideConfiguration.getDefaultServerResource() == null &&
            serverSideConfiguration.getResourcePools().isEmpty();
  }

  /**
   * Checks whether the {@link ServerSideConfiguration} sent from the client is equal with the ServerSideConfiguration
   * that is already configured on the server.
   * @param serverSideConfiguration the ServerSideConfiguration to be validated.  This is sent from a client
   * @param errorMsg
   * @return
   */
  private boolean sharedResourcePoolsEqual(ServerSideConfiguration serverSideConfiguration, StringBuilder errorMsg) {
    //validate that both ServerSideConfiguration's have the same keyset and thus the same pool names and the same number of pools.
    if(!this.sharedResourcePools.keySet().equals(serverSideConfiguration.getResourcePools().keySet())) {
      errorMsg.append("pool names not equal. Client sent pool names: ");
      errorMsg.append(serverSideConfiguration.getResourcePools().keySet().toString());
      errorMsg.append(" Server pool names: ");
      errorMsg.append(this.sharedResourcePools.keySet().toString());
      return false;
    }

    //verify client ServerSideConfiguration sent contains the same Pools as the configured ServerSideConfiguration.
    for(String poolName : serverSideConfiguration.getResourcePools().keySet()) {
      //already validated that keySets are equal so no need to check if null
      ResourcePageSource resourcePageSource = this.sharedResourcePools.get(poolName);

      if( (!resourcePageSource.pool.source().equalsIgnoreCase(serverSideConfiguration.getResourcePools().get(poolName).source())) ||
          (resourcePageSource.pool.size() != serverSideConfiguration.getResourcePools().get(poolName).size()) ) {
          errorMsg.append("ServerSideConfiguration pool sent by client is different than server ServerSideConfiguration pool.");
          return false;
      }
    }

    return true;
  }

  private static boolean nullSafeEquals(String s1, String s2) {
    return (s1 == null ? s2 == null : s1.equals(s2));
  }

  /**
   * Handles the {@link CreateServerStore CreateServerStore} message.  This message is used by a client to
   * create a new {@link ServerStore}; if the {@code ServerStore} exists, a failure is returned to the client.
   * Once created, the client is registered with the {@code ServerStore}.  The registration persists until
   * the client disconnects or explicitly releases the store.
   * <p>
   *   If the store uses a fixed resource, this method allocates a new fixed resource pool associated
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
   *
   * @return an {@code EhcacheEntityResponse} indicating the success or failure of the store creation operation
   */
  private EhcacheEntityResponse createServerStore(ClientDescriptor clientDescriptor, CreateServerStore createServerStore) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not connected to the Clustered Store Manager"));
    }
    if (!clientState.isAttached()) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not attached to the Clustered Store Manager"));
    }
    if (!isConfigured()) {
      return responseFactory.failure(new IllegalStateException("Clustered Store Manager is not configured"));
    }

    String name = createServerStore.getName();    // client cache identifier/name

    LOGGER.info("Client {} creating new server-side store '{}'", clientDescriptor, name);

    if (stores.containsKey(name)) {
      return responseFactory.failure(new IllegalStateException("Store '" + name + "' already exists"));
    }

    ServerStoreConfiguration storeConfiguration = createServerStore.getStoreConfiguration();
    ResourcePageSource resourcePageSource;
    PoolAllocation allocation = storeConfiguration.getPoolAllocation();
    if (allocation instanceof PoolAllocation.Fixed) {
      /*
       * Fixed allocation pools are taken directly from a specified resource, not a shared pool, and
       * identified by the cache identifier/name.
       */
      if (fixedResourcePools.containsKey(name)) {
        return responseFactory.failure(new IllegalStateException("Fixed resource pool for store '" + name + "' already exists"));

      } else {
        PoolAllocation.Fixed fixedAllocation = (PoolAllocation.Fixed)allocation;
        try {
          String resourceName = fixedAllocation.getResourceName();
          if (resourceName == null) {
            if (defaultServerResource == null) {
              return responseFactory.failure(new IllegalStateException("Fixed pool for store '" + name
                  + "' not defined; default server resource not configured"));
            } else {
              resourceName = defaultServerResource;
            }
          }
          resourcePageSource = createPageSource(name, new Pool(resourceName, fixedAllocation.getSize()));
        } catch (RuntimeException e) {
          return responseFactory.failure(e);
        }
        fixedResourcePools.put(name, resourcePageSource);
      }

    } else if (allocation instanceof PoolAllocation.Shared) {
      /*
       * Shared allocation pools are created during EhcacheActiveEntity configuration.
       */
      PoolAllocation.Shared sharedAllocation = (PoolAllocation.Shared)allocation;
      resourcePageSource = sharedResourcePools.get(sharedAllocation.getResourcePoolName());

    } else {
      final String msg = "Unexpected PoolAllocation type: " + allocation.getClass().getName();
      final IllegalStateException cause = new IllegalStateException(msg);
      LOGGER.error(msg, cause);
      return responseFactory.failure(cause);
    }

    stores.put(name, new ServerStoreImpl(storeConfiguration, resourcePageSource));
    attachStore(clientDescriptor, name);
    return responseFactory.success();
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
   *
   * @return an {@code EhcacheEntityResponse} indicating the success or failure of the store attachment operation
   */
  private EhcacheEntityResponse validateServerStore(ClientDescriptor clientDescriptor, ValidateServerStore validateServerStore) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not connected to the Clustered Store Manager"));
    }
    if (!clientState.isAttached()) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not attached to the Clustered Store Manager"));
    }
    if (!isConfigured()) {
      return responseFactory.failure(new IllegalStateException("Clustered Store Manager is not configured"));
    }

    String name = validateServerStore.getName();
    ServerStoreConfiguration clientConfiguration = validateServerStore.getStoreConfiguration();

    LOGGER.info("Client {} validating server-side store '{}'", clientDescriptor, name);
    ServerStoreImpl store = stores.get(name);
    if (store != null) {
      try {
        storeCompatibility.verify(store.getStoreConfiguration(), clientConfiguration);
      } catch (ClusteredStoreValidationException e) {
        return responseFactory.failure(e);
      }
      attachStore(clientDescriptor, name);
      return responseFactory.success();
    } else {
      return responseFactory.failure(new IllegalStateException("Store '" + name + "' does not exist"));
    }
  }

  /**
   * Handles the {@link ReleaseServerStore ReleaseServerStore} message.  This message is used by a client to
   * explicitly release its attachment to a {@code ServerStore}.  If the client is not registered with the store,
   * a failure is returned to the client.
   *
   * @param clientDescriptor the client identifier requesting store release
   * @param releaseServerStore the {@code ReleaseServerStore} message identifying the store to release
   *
   * @return an {@code EhcacheActiveResponse} indicating the success or failure of the store release
   */
  private EhcacheEntityResponse releaseServerStore(ClientDescriptor clientDescriptor, ReleaseServerStore releaseServerStore) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not connected to the Clustered Store Manager"));
    }
    if (!clientState.isAttached()) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not attached to the Clustered Store Manager"));
    }
    if (!isConfigured()) {
      return responseFactory.failure(new IllegalStateException("Clustered Store Manager is not configured"));
    }

    String name = releaseServerStore.getName();

    LOGGER.info("Client {} releasing server-side store '{}'", clientDescriptor, name);
    ServerStoreImpl store = stores.get(name);
    if (store != null) {

      boolean removedFromClient = clientState.removeStore(name);
      removedFromClient |= detachStore(clientDescriptor, name);

      if (!removedFromClient) {
        return responseFactory.failure(new IllegalStateException("Store '" + name + "' is not in use by client"));
      }

      return responseFactory.success();
    } else {
      return responseFactory.failure(new IllegalStateException("Store '" + name + "' does not exist"));
    }
  }

  /**
   * Handles the {@link DestroyServerStore DestroyServerStore} message.  This message is used by a client to
   * explicitly destroy a {@link ServerStore} instance.  If the {@code ServerStore} does not exist or a client
   * is attached to the store, a failure is returned to the client.
   *
   * @param clientDescriptor the client identifier requesting store destruction
   * @param destroyServerStore the {@code DestroyServerStore} message identifying the store to release
   *
   * @return an {@code EhcacheEntityResponse} indicating success or failure of the store destruction
   */
  private EhcacheEntityResponse destroyServerStore(ClientDescriptor clientDescriptor, DestroyServerStore destroyServerStore) {
    ClientState clientState = this.clientStateMap.get(clientDescriptor);
    if (clientState == null) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not connected to the Clustered Store Manager"));
    }
    if (!clientState.isAttached()) {
      return responseFactory.failure(new IllegalStateException("Client " + clientDescriptor + " is not attached to the Clustered Store Manager"));
    }
    if (!isConfigured()) {
      return responseFactory.failure(new IllegalStateException("Clustered Store Manager is not configured"));
    }

    String name = destroyServerStore.getName();

    final Set<ClientDescriptor> clients = storeClientMap.get(name);
    if (clients != null && !clients.isEmpty()) {
      return responseFactory.failure(new IllegalStateException(
          "Can not destroy server-side store '" + name + "': in use by " + clients.size() + " clients"));
    }

    LOGGER.info("Client {} destroying server-side store '{}'", clientDescriptor, name);
    final ServerStoreImpl store = stores.remove(name);
    if (store == null) {
      return responseFactory.failure(new IllegalStateException("Store '" + name + "' does not exist"));
    } else {
      /*
       * A ServerStore using a fixed resource pool is the only referent to that pool.  When such a
       * ServerStore is destroyed, the associated fixed resource pool must also be discarded.
       */
      ResourcePageSource expectedPageSource = fixedResourcePools.get(name);
      if (expectedPageSource != null) {
        if (store.getPageSource() == expectedPageSource) {
          fixedResourcePools.remove(name);
          releasePool("fixed", name, expectedPageSource);
        } else {
          LOGGER.error("Client {} attempting to destroy server-side store '{}' with unmatched page source", clientDescriptor, name);
        }
      }

      // TODO: ServerStore closure here ...
      storeClientMap.remove(name);
      return responseFactory.success();
    }
  }

  private Map<String, ResourcePageSource> createPools(Map<String, Pool> resourcePools) {
    Map<String, ResourcePageSource> pools = new HashMap<String, ResourcePageSource>();
    try {
      for (Entry<String, Pool> e : resourcePools.entrySet()) {
        pools.put(e.getKey(), createPageSource(e.getKey(), e.getValue()));
      }
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

  private ResourcePageSource createPageSource(String poolName, Pool pool) {
    ResourcePageSource pageSource;
    OffHeapResource source = services.getService(OffHeapResourceIdentifier.identifier(pool.source()));
    if (source == null) {
      throw new IllegalArgumentException("Non-existent server side resource '" + pool.source() + "'");
    } else if (source.reserve(pool.size())) {
      try {
        pageSource = new ResourcePageSource(pool);
      } catch (RuntimeException t) {
        source.release(pool.size());
        throw new IllegalArgumentException("Failure allocating pool " + pool, t);
      }
      LOGGER.info("Reserved {} bytes from resource '{}' for pool '{}'", pool.size(), pool.source(), poolName);
    } else {
      throw new IllegalArgumentException("Insufficient defined resources to allocate pool " + poolName + "=" + pool);
    }
    return pageSource;
  }

  private void releasePools(String poolType, Map<String, ResourcePageSource> resourcePools) {
    if (resourcePools == null) {
      return;
    }
    final Iterator<Entry<String, ResourcePageSource>> fixedPoolIterator = resourcePools.entrySet().iterator();
    while (fixedPoolIterator.hasNext()) {
      Entry<String, ResourcePageSource> poolEntry = fixedPoolIterator.next();
      releasePool(poolType, poolEntry.getKey(), poolEntry.getValue());
      fixedPoolIterator.remove();
    }
  }

  private void releasePool(String poolType, String poolName, ResourcePageSource resourcePageSource) {
    Pool pool = resourcePageSource.getPool();
    OffHeapResource source = services.getService(OffHeapResourceIdentifier.identifier(pool.source()));
    if (source != null) {
      source.release(pool.size());
      LOGGER.info("Released {} bytes from resource '{}' for {} pool '{}'", pool.size(), pool.source(), poolType, poolName);
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

    LOGGER.info("Client {} attached to store '{}'", clientDescriptor, storeId);
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
      LOGGER.info("Client {} detached from store '{}'", clientDescriptor, storeId);
    }

    return wasRegistered;
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
      this.delegatePageSource = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), pool.size(), GIGABYTES.toBytes(1), MEGABYTES.toBytes(128));
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
