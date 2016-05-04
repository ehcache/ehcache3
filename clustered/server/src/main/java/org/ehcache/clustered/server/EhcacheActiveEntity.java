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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

import org.ehcache.clustered.common.ClusteredStoreValidationException;
import org.ehcache.clustered.common.ServerStoreCompatibility;
import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.ConfigureCacheManager;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.CreateServerStore;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.DestroyServerStore;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.ValidateServerStore;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.ValidateCacheManager;
import org.ehcache.clustered.common.messages.EhcacheEntityResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.terracotta.entity.ActiveServerEntity;
import org.terracotta.entity.ClientDescriptor;
import org.terracotta.entity.PassiveSynchronizationChannel;
import org.terracotta.entity.ServiceRegistry;
import org.terracotta.offheapresource.OffHeapResource;
import org.terracotta.offheapresource.OffHeapResourceIdentifier;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import static java.util.Collections.unmodifiableMap;
import static org.terracotta.offheapstore.util.MemoryUnit.GIGABYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

import static org.ehcache.clustered.common.messages.EhcacheEntityResponse.failure;
import static org.ehcache.clustered.common.messages.EhcacheEntityResponse.success;

public class EhcacheActiveEntity implements ActiveServerEntity<EhcacheEntityMessage, EhcacheEntityResponse> {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhcacheActiveEntity.class);

  private final UUID identity;
  private final ServiceRegistry services;

  private Map<String, PageSource> resourcePools;
  private Map<String, ServerStore> stores;

  private final Map<ClientDescriptor, Set<String>> clientStoreMap = new HashMap<ClientDescriptor, Set<String>>();
  private final ConcurrentHashMap<String, Set<ClientDescriptor>> storeClientMap =
      new ConcurrentHashMap<String, Set<ClientDescriptor>>();

  private final ServerStoreCompatibility storeCompatibility = new ServerStoreCompatibility();

  EhcacheActiveEntity(ServiceRegistry services, byte[] config) {
    this.identity = ClusteredEhcacheIdentity.deserialize(config);
    this.services = services;
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
    for (Map.Entry<ClientDescriptor, Set<String>> entry : clientStoreMap.entrySet()) {
      clientMap.put(entry.getKey(), Collections.unmodifiableSet(new HashSet<String>(entry.getValue())));
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

  /**
   * Gets the set of defined resource pools.
   *
   * @return an unmodifiable set of resource pool identifiers
   */
  // This method is intended for unit test use; modifications are likely needed for other (monitoring) purposes
  // TODO: Provide some mechanism to report on storage utilization -- PageSource provides little visibility
  Set<String> getResourcePoolIds() {
    return Collections.unmodifiableSet(new HashSet<String>(resourcePools.keySet()));
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    LOGGER.info("Connecting {}", clientDescriptor);
    if (!clientStoreMap.containsKey(clientDescriptor)) {
      clientStoreMap.put(clientDescriptor, new HashSet<String>());
    } else {
      // This is logically an AssertionError
      LOGGER.error("Client {} already registered as connected", clientDescriptor);
    }
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    LOGGER.info("Disconnecting {}", clientDescriptor);
    Set<String> storeIds = clientStoreMap.remove(clientDescriptor);
    if (storeIds == null) {
      // This is logically an AssertionError
      LOGGER.error("Client {} not registered as connected", clientDescriptor);
    } else {
      for (String storeId : storeIds) {
        boolean updated = false;
        while (!updated) {
          Set<ClientDescriptor> clients = storeClientMap.get(storeId);
          if (clients != null && clients.contains(clientDescriptor)) {
            Set<ClientDescriptor> newClients = new HashSet<ClientDescriptor>(clients);
            newClients.remove(clientDescriptor);
            updated = storeClientMap.replace(storeId, clients, newClients);
          } else {
            updated = true;
          }
        }
      }
    }
  }

  @Override
  public EhcacheEntityResponse invoke(ClientDescriptor clientDescriptor, EhcacheEntityMessage message) {
    switch (message.getType()) {
      case CONFIGURE: return configure((ConfigureCacheManager) message);
      case VALIDATE: return validate((ValidateCacheManager) message);
      case CREATE_SERVER_STORE: return createServerStore(clientDescriptor, (CreateServerStore) message);
      case VALIDATE_SERVER_STORE: return validateServerStore(clientDescriptor, (ValidateServerStore) message);
      case DESTROY_SERVER_STORE: return destroyServerStore(clientDescriptor, (DestroyServerStore) message);
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

  @Override
  public void destroy() {
    //nothing to do
  }

  private EhcacheEntityResponse configure(ConfigureCacheManager message) throws IllegalStateException {
    if (resourcePools == null) {
      LOGGER.info("Configuring server-side cache manager");
      try {
        this.resourcePools = createPools(message.getConfiguration().getResourcePools());
      } catch (RuntimeException e) {
        return failure(e);
      }
      this.stores = new HashMap<String, ServerStore>();
      return success();
    } else {
      return failure(new IllegalStateException("Clustered Cache Manager already configured"));
    }
  }

  private EhcacheEntityResponse validate(ValidateCacheManager message)  throws IllegalArgumentException {
    if (!this.resourcePools.keySet().equals(message.getConfiguration().getResourcePools().keySet())) {
      return failure(new IllegalArgumentException("ResourcePools not aligned"));
    } else {
      return success();
    }
  }

  private Map<String, PageSource> createPools(Map<String, Pool> resourcePools) {
    Map<String, PageSource> pools = new HashMap<String, PageSource>();
    for (Entry<String, Pool> e : resourcePools.entrySet()) {
      Pool pool = e.getValue();
      OffHeapResource source = services.getService(OffHeapResourceIdentifier.identifier(pool.source()));
      if (source == null) {
        throw new IllegalArgumentException("Non-existent server side resource '" + pool.source() + "'");
      } else if (source.reserve(pool.size())) {
        try {
          pools.put(e.getKey(), new UpfrontAllocatingPageSource(new OffHeapBufferSource(), pool.size(), GIGABYTES.toBytes(1), MEGABYTES.toBytes(128)));
        } catch (Throwable t) {
          source.release(pool.size());
          throw new IllegalArgumentException("Failure allocating pool " + pool, t);
        }
      } else {
        throw new IllegalArgumentException("Insufficient defined resources to allocate pool " + e);
      }
    }
    return unmodifiableMap(pools);
  }

  private EhcacheEntityResponse createServerStore(ClientDescriptor clientDescriptor, CreateServerStore createServerStore) {
    String name = createServerStore.getName();
    ServerStoreConfiguration storeConfiguration = createServerStore.getStoreConfiguration();

    // TODO: Connect ServerStore to local resourcePool!

    LOGGER.info("Creating new server-side store for '{}'", name);
    if (stores.containsKey(name)) {
      return failure(new IllegalStateException("Store '" + name + "' already exists"));
    } else {
      stores.put(name, new ServerStore(storeConfiguration));
      attachStore(clientDescriptor, name);
      return success();
    }
  }

  private EhcacheEntityResponse validateServerStore(ClientDescriptor clientDescriptor, ValidateServerStore validateServerStore) {
    String name = validateServerStore.getName();
    ServerStoreConfiguration clientConfiguration = validateServerStore.getStoreConfiguration();

    LOGGER.info("Validating server-side store for '{}'", name);
    ServerStore store = stores.get(name);
    if (store != null) {
      try {
        storeCompatibility.verify(store.getStoreConfiguration(), clientConfiguration);
      } catch (ClusteredStoreValidationException e) {
        return failure(e);
      }
      attachStore(clientDescriptor, name);
      return success();
    } else {
      return failure(new IllegalStateException("Store '" + name + "' does not exist"));
    }
  }

  /**
   * Establishes a registration of a client against a store.
   * <p>
   *   Once registered, a client is associated with a store until the client disconnects.
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
        updated = storeClientMap.replace(storeId, clients, newClients);

      } else {
        // Already registered
        updated = true;
      }
    }

    final Set<String> storeIds = clientStoreMap.get(clientDescriptor);
    storeIds.add(storeId);

    LOGGER.info("Client {} attached to store '{}'", clientDescriptor, storeId);
  }

  private EhcacheEntityResponse destroyServerStore(ClientDescriptor clientDescriptor, DestroyServerStore destroyServerStore) {
    String name = destroyServerStore.getName();

    final Set<ClientDescriptor> clients = storeClientMap.get(name);
    if (clients != null && !(clients.isEmpty() || clients.equals(Collections.singleton(clientDescriptor)))) {
      return failure(new IllegalStateException(
          "Can not destroy server-side store '" + name + "': in use by " + clients.size() + " clients"));
    }

    LOGGER.info("Destroying server-side store '{}' by client {}", name, clientDescriptor);
    if (stores.remove(name) == null) {
      return failure(new IllegalStateException("Store '" + name + "' does not exist"));
    } else {
      return success();
    }
  }
}
