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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.internal.store.ServerStoreProxy;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.PersistableResourceService;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Provides support for accessing server-based resources.
 */
public interface ClusteringService extends PersistableResourceService {

  ClusteringServiceConfiguration getConfiguration();

  /**
   * Gets a {@link ServerStoreProxy} though which a server-resident {@code ServerStore} is accessed.
   *
   * @param cacheIdentifier the {@code ClusteredCacheIdentifier} for the cache for which a
   *                        {@code ServerStoreProxy} is requested
   * @param storeConfig the configuration used for the {@link Store} for which the {@code ServerStoreProxy}
   *                    is requested
   * @param <K> the cache-exposed key type
   * @param <V> the cache-exposed value type
   *
   * @return a new {@code ServerStoreProxy}
   */
  <K, V> ServerStoreProxy<K, V> getServerStoreProxy(ClusteredCacheIdentifier cacheIdentifier, final Store.Configuration<K, V> storeConfig);

  void connect();

  /**
   * Identifies a client-side cache to server-based components.
   */
  interface ClusteredCacheIdentifier extends ServiceConfiguration<ClusteringService> {
    String getId();
  }
}
