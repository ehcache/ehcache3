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

import java.util.Comparator;
import org.ehcache.Cache;
import org.ehcache.function.Predicate;
import org.ehcache.spi.cache.Store;

/**
 *
 * @author Chris Dennis
 */
public class StoreConfigurationImpl<K, V> implements Store.Configuration<K, V> {
  
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Comparable<Long> capacityConstraint;
  private final Predicate<Cache.Entry<K, V>> evictionVeto;
  private final Comparator<Cache.Entry<K, V>> evictionPrioritizer;
  private final ClassLoader classLoader;

  public StoreConfigurationImpl(CacheConfiguration<K, V> cacheConfig) {
    this(cacheConfig.getKeyType(), cacheConfig.getValueType(), cacheConfig.getCapacityConstraint(),
            cacheConfig.getEvictionVeto(), cacheConfig.getEvictionPrioritizer(), cacheConfig.getClassLoader());
  }

  public StoreConfigurationImpl(Class<K> keyType, Class<V> valueType, ClassLoader classLoader) {
    this(keyType, valueType, null, null, null, classLoader);
  }
          
  public StoreConfigurationImpl(Class<K> keyType, Class<V> valueType, Comparable<Long> capacityConstraint,
          Predicate<Cache.Entry<K, V>> evictionVeto, Comparator<Cache.Entry<K, V>> evictionPrioritizer,
          ClassLoader classLoader) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.capacityConstraint = capacityConstraint;
    this.evictionVeto = evictionVeto;
    this.evictionPrioritizer = evictionPrioritizer;
    this.classLoader = classLoader;
  }

  @Override
  public Class<K> getKeyType() {
    return keyType;
  }

  @Override
  public Class<V> getValueType() {
    return valueType;
  }

  @Override
  public Comparable<Long> getCapacityConstraint() {
    return capacityConstraint;
  }

  @Override
  public Predicate<Cache.Entry<K, V>> getEvictionVeto() {
    return evictionVeto;
  }

  @Override
  public Comparator<Cache.Entry<K, V>> getEvictionPrioritizer() {
    return evictionPrioritizer;
  }

  @Override
  public ClassLoader getClassLoader() {
    return this.classLoader;
  }
}
