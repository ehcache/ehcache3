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

package org.ehcache.config;

import org.ehcache.expiry.Expiry;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public class TerracottaBaseCacheConfiguration<K, V> extends BaseCacheConfiguration<K, V> implements TerracottaCacheConfiguration<K, V> {

  private ClusteredCacheSharedConfiguration<K, V> cacheSharedConfiguration;

  public TerracottaBaseCacheConfiguration(final Class<K> keyType, final Class<V> valueType,
                                          final EvictionVeto<? super K, ? super V> evictionVeto,
                                          final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer,
                                          final Expiry<? super K, ? super V> expiry,
                                          final ClassLoader classLoader,
                                          final ResourcePools resourcePools,
                                          final ServiceConfiguration<?>... serviceConfigurations) {
    super(keyType, valueType, evictionVeto, evictionPrioritizer, expiry,
        classLoader, resourcePools, serviceConfigurations);
    cacheSharedConfiguration = null; // TODO: Means default to what's on the server?
  }

  public TerracottaBaseCacheConfiguration(final ClusteredCacheSharedConfiguration<K, V> cacheSharedConfiguration,
                                          final ClassLoader classLoader,
                                          final ResourcePools resourcePools,
                                          final ServiceConfiguration<?>... serviceConfigurations) {
    super(cacheSharedConfiguration.getKeyType(),
        cacheSharedConfiguration.getValueType(),
        cacheSharedConfiguration.getEvictionVeto(),
        cacheSharedConfiguration.getEvictionPrioritizer(),
        cacheSharedConfiguration.getExpiry(),
        classLoader, resourcePools, serviceConfigurations);
    this.cacheSharedConfiguration = cacheSharedConfiguration;
  }

  // TODO Override all getters to first check whether to delegate to the cacheSharedConfiguration ?!

  @Override
  public ClusteredCacheSharedConfiguration<K, V> getClusteredCacheConfiguration() {
    return cacheSharedConfiguration;
  }
}
