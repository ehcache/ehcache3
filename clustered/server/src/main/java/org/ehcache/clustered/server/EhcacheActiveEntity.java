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

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.ClusteredEhcacheIdentity;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.ConfigureCacheManager;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.CreateServerStore;
import org.ehcache.clustered.common.messages.EhcacheEntityMessage.DestroyServerStore;
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

  EhcacheActiveEntity(ServiceRegistry services, byte[] config) {
    this.identity = ClusteredEhcacheIdentity.deserialize(config);
    this.services = services;
  }

  @Override
  public void connected(ClientDescriptor clientDescriptor) {
    //nothing to do
  }

  @Override
  public void disconnected(ClientDescriptor clientDescriptor) {
    //nothing to do
  }

  @Override
  public EhcacheEntityResponse invoke(ClientDescriptor clientDescriptor, EhcacheEntityMessage message) {
    switch (message.getType()) {
      case CONFIGURE: return configure((ConfigureCacheManager) message);
      case VALIDATE: return validate((ValidateCacheManager) message);
      case CREATE_SERVER_STORE: return createServerStore((CreateServerStore) message);
      case DESTROY_SERVER_STORE: return destroyServerStore((DestroyServerStore) message);
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

  // QUESTION: Should this be getOrCreateServerStore?
  private EhcacheEntityResponse createServerStore(CreateServerStore createServerStore) {
    String name = createServerStore.getName();
    ServerStoreConfiguration storeConfiguration = new ServerStoreConfiguration(
        createServerStore.getStoredKeyType(),
        createServerStore.getStoredValueType(),
        createServerStore.getActualKeyType(),
        createServerStore.getActualValueType(),
        createServerStore.getKeySerializerType(),
        createServerStore.getValueSerializerType()
    );
    LOGGER.info("Creating new server-side store for '{}'", name);
    if (stores.containsKey(name)) {
      return failure(new IllegalStateException("Store already exists"));
    } else {
      stores.put(name, new ServerStore(storeConfiguration));
      return success();
    }
  }

  private EhcacheEntityResponse destroyServerStore(DestroyServerStore destroyServerStore) {
    String name = destroyServerStore.getName();
    LOGGER.info("Destroying server-side store '{}'", name);
    if (stores.remove(name) == null) {
      return failure(new IllegalStateException("Store doesn't exist"));
    } else {
      return success();
    }
  }
}
