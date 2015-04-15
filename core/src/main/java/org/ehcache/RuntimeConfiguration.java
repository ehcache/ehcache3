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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/**
 * @author rism
 */
public class RuntimeConfiguration<K, V> implements CacheRuntimeConfiguration<K, V> {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final EvictionVeto<? super K, ? super V> evictionVeto;
  private final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer;
  private final ClassLoader classLoader;
  private final Expiry<? super K, ? super V> expiry;
  private final PersistenceMode persistenceMode;
  private final ResourcePools resourcePools;
  private InternalRuntimeConfigurationImpl<K, V> internalRuntimeConfiguration;

  public RuntimeConfiguration(CacheConfiguration<K, V> config,
                              InternalRuntimeConfigurationImpl<K, V> internalRuntimeConfiguration) {
    this.serviceConfigurations = copy(config.getServiceConfigurations());
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.evictionVeto = config.getEvictionVeto();
    this.evictionPrioritizer = config.getEvictionPrioritizer();
    this.classLoader = config.getClassLoader();
    this.expiry = config.getExpiry();
    this.persistenceMode = config.getPersistenceMode();
    this.resourcePools = config.getResourcePools();
    this.internalRuntimeConfiguration = internalRuntimeConfiguration;
  }


  @Override
  public Collection<ServiceConfiguration<?>> getServiceConfigurations() {
    return this.serviceConfigurations;
  }

  @Override
  public Class<K> getKeyType() {
    return this.keyType;
  }

  @Override
  public Class<V> getValueType() {
    return this.valueType;
  }

  @Override
  public EvictionVeto<? super K, ? super V> getEvictionVeto() {
    return this.evictionVeto;
  }

  @Override
  public EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer() {
    return this.evictionPrioritizer;
  }

  @Override
  public ClassLoader getClassLoader() {
    return this.classLoader;
  }

  @Override
  public Expiry<? super K, ? super V> getExpiry() {
    return expiry;
  }

  @Override
  public PersistenceMode getPersistenceMode() {
    return this.persistenceMode;
  }

  @Override
  public ResourcePools getResourcePools() {
    return this.resourcePools;
  }

  @Override
  public synchronized void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
    internalRuntimeConfiguration.deregisterCacheEventListener(listener);
  }

  @Override
  public synchronized void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering,
                                                      EventFiring firing, Set<EventType> forEventTypes) {
    internalRuntimeConfiguration.registerCacheEventListener(listener, ordering, firing, forEventTypes);
  }


  @Override
  public void setCapacityConstraint(Comparable<Long> constraint) {
    internalRuntimeConfiguration.setCapacityConstraint(constraint);
  }

  @Override
  public void releaseAllEventListeners() {
    internalRuntimeConfiguration.releaseAllEventListeners();
  }

  private <T> Collection<T> copy(Collection<T> collection) {
    if (collection == null) {
      return null;
    }

    return Collections.unmodifiableCollection(new ArrayList<T>(collection));
  }
}