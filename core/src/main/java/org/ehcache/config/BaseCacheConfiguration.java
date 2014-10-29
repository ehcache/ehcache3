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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

import org.ehcache.Cache;
import org.ehcache.event.CacheEventListener;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.Predicate;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Alex Snaps
 */
public class BaseCacheConfiguration<K, V> implements CacheConfiguration<K,V> {

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final Comparable<Long> capacityConstraint;
  private final Predicate<Cache.Entry<K, V>> evictionVeto;
  private final Comparator<Cache.Entry<K, V>> evictionPrioritizer;
  private final Collection<ServiceConfiguration<?>> serviceConfigurations;
  private final SerializationProvider serializationProvider;
  private final ClassLoader classLoader;
  private final Expiry<K, V> expiry;

  public BaseCacheConfiguration(Class<K> keyType, Class<V> valueType, Comparable<Long> capacityConstraint,
          Predicate<Cache.Entry<K, V>> evictionVeto, Comparator<Cache.Entry<K, V>> evictionPrioritizer,
          ClassLoader classLoader, Expiry<K, V> expiry, SerializationProvider serializationProvider, ServiceConfiguration<?>... serviceConfigurations) {
    this.keyType = keyType;
    this.valueType = valueType;
    this.capacityConstraint = capacityConstraint;
    this.evictionVeto = evictionVeto;
    this.evictionPrioritizer = evictionPrioritizer;
    this.serializationProvider = serializationProvider;
    this.classLoader = classLoader;
    this.expiry = expiry;
    this.serviceConfigurations = Collections.unmodifiableCollection(Arrays.asList(serviceConfigurations));
  }

  @Override
  public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
    return serviceConfigurations;
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
  public Set<CacheEventListener<?, ?>> getEventListeners() {
    // XXX:
    return Collections.emptySet();
  }

  @Override
  public SerializationProvider getSerializationProvider() {
    return serializationProvider;
  }

  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }  
  
  @Override
  public Expiry<K, V> getExpiry() {
    return expiry;
  }
}
