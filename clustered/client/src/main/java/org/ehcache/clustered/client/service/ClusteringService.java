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

package org.ehcache.clustered.client.service;

import org.ehcache.CachePersistenceException;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy.ServerCallback;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.Configuration;
import org.ehcache.spi.persistence.PersistableResourceService;

/**
 * Provides support for accessing server-based resources.
 */
public interface ClusteringService extends PersistableResourceService {

  ClusteringServiceConfiguration getConfiguration();

  /**
   * @return true if a connection to a cluster exists
   */
  boolean isConnected();

  /**
   * Gets a {@link ServerStoreProxy} though which a server-resident {@code ServerStore} is accessed.
   *
   * @param <K> the cache-exposed key type
   * @param <V> the cache-exposed value type
   *
   * @param cacheIdentifier the {@code ClusteredCacheIdentifier} for the cache for which a
   *                        {@link ServerStoreProxy} is requested
   * @param storeConfig the configuration used for the {@link Store} for which the {@link ServerStoreProxy}
   *                    is requested
   * @param consistency the store's consistency
   * @return a new {@link ServerStoreProxy}
   *
   * @throws CachePersistenceException if the {@code cacheIdentifier} is unknown or the {@code ServerStoreProxy} cannot be created
   */
  <K, V> ServerStoreProxy getServerStoreProxy(ClusteredCacheIdentifier cacheIdentifier, final Configuration<K, V> storeConfig,
                                              Consistency consistency, ServerCallback invalidation) throws CachePersistenceException;

  /**
   * Releases access to a {@link ServerStoreProxy} and the server-resident {@code ServerStore} it represents.
   *
   * @param serverStoreProxy a {@link ServerStoreProxy} obtained through {@link #getServerStoreProxy}
   * @param isReconnect whether client is trying to reconnect
   */
  void releaseServerStoreProxy(ServerStoreProxy serverStoreProxy, boolean isReconnect);

  /**
   * Add a block to execute when the connection is recovered after it was closed.
   *
   * @param runnable the execution block
   */
  void addConnectionRecoveryListener(Runnable runnable);

  /**
   * Remove a block to execute when the connection is recovered after it was closed.
   *
   * @param runnable the execution block
   */
  void removeConnectionRecoveryListener(Runnable runnable);

  /**
   * A {@link org.ehcache.spi.persistence.PersistableResourceService.PersistenceSpaceIdentifier PersistenceSpaceIdentifier}
   * that can provide an id.
   */
  interface ClusteredCacheIdentifier extends PersistenceSpaceIdentifier<ClusteringService> {

    /**
     * The id associated with this identifier.
     *
     * @return an id
     */
    String getId();
  }


}
