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

package org.ehcache;

import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.config.StandaloneCacheConfiguration;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.cache.Store;

/**
 * @author Alex Snaps
 */
public class StandaloneCacheBuilder<K, V, T extends StandaloneCache<K, V>> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private Comparable<Long> capacityConstraint;
  
  public StandaloneCacheBuilder(final Class<K> keyType, final Class<V> valueType) {
    this.keyType = keyType;
    this.valueType = valueType;
  }

  T build(ServiceLocator serviceLocator) {
    Store.Provider storeProvider = serviceLocator.findService(Store.Provider.class);
    if (capacityConstraint == null) {
      return (T) new Ehcache(storeProvider.createStore(new StoreConfigurationImpl(keyType, valueType)));
    } else {
      return (T) new Ehcache(storeProvider.createStore(new StoreConfigurationImpl(keyType, valueType, capacityConstraint)));
    }
  }

  public final T build() {
    return build(new ServiceLocator());
  }

  public final <N extends T> StandaloneCacheBuilder<K, V, N> with(StandaloneCacheConfiguration<K, V, N> cfg) {
    return cfg.builder(this);
  }

  public final StandaloneCacheBuilder<K, V, T> withCapacity(Comparable<Long> constraint) {
    capacityConstraint = constraint;
    return this;
  }
  
  public static <K, V, T extends StandaloneCache<K, V>> StandaloneCacheBuilder<K, V, T> newCacheBuilder(Class<K> keyType, Class<V> valueType) {
    return new StandaloneCacheBuilder<K, V, T>(keyType, valueType);
  }

}
