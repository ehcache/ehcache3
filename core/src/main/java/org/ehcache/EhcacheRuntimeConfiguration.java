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
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.EventListenerWrapper;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ResourcePoolMerger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author rism
 */
class EhcacheRuntimeConfiguration<K, V> implements CacheRuntimeConfiguration<K, V>, InternalRuntimeConfiguration {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations;
  private final CacheConfiguration<? super K, ? super V> config;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final EvictionVeto<? super K, ? super V> evictionVeto;
  private final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer;
  private final ClassLoader classLoader;
  private final Expiry<? super K, ? super V> expiry;
  private volatile ResourcePools resourcePools;
  private final CacheEventNotificationService<K, V> eventNotificationService;

  private final List<CacheConfigurationChangeListener> cacheConfigurationListenerList
      = new CopyOnWriteArrayList<CacheConfigurationChangeListener>();

  public EhcacheRuntimeConfiguration(CacheConfiguration<K, V> config,
                                     CacheEventNotificationService<K, V> eventNotifier) {
    this.config = config;
    this.serviceConfigurations = copy(config.getServiceConfigurations());
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.evictionVeto = config.getEvictionVeto();
    this.evictionPrioritizer = config.getEvictionPrioritizer();
    this.classLoader = config.getClassLoader();
    this.expiry = config.getExpiry();
    this.resourcePools = config.getResourcePools();
    this.eventNotificationService = eventNotifier;
  }

  @Override
  public synchronized void updateResourcePools(ResourcePools pools) {

    if(pools == null) {
      throw new NullPointerException("Pools to be updated cannot be null");
    }

    ResourcePoolMerger poolMerger = new ResourcePoolMerger();
    ResourcePools updatedResourcePools = poolMerger.validateAndMerge(config.getResourcePools(), pools);
    fireCacheConfigurationChange(CacheConfigurationProperty.UPDATESIZE, config.getResourcePools(), updatedResourcePools);
    this.resourcePools = updatedResourcePools;
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
  public ResourcePools getResourcePools() {
    return this.resourcePools;
  }

  @Override
  public boolean addCacheConfigurationListener(List<CacheConfigurationChangeListener> listeners) {
    return this.cacheConfigurationListenerList.addAll(listeners);
  }

  @Override
  public boolean removeCacheConfigurationListener(CacheConfigurationChangeListener listener) {
    return this.cacheConfigurationListenerList.remove(listener);
  }

  @Override
  public synchronized void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
    fireCacheConfigurationChange(CacheConfigurationProperty.REMOVELISTENER, listener, listener);
  }

  @Override
  public synchronized void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering,
                                                      EventFiring firing, Set<EventType> forEventTypes) {
    EventListenerWrapper listenerWrapper = new EventListenerWrapper(listener, firing, ordering, EnumSet.copyOf(forEventTypes));
    fireCacheConfigurationChange(CacheConfigurationProperty.ADDLISTENER, listenerWrapper, listenerWrapper);
  }

  private <T> Collection<T> copy(Collection<T> collection) {
    if (collection == null) {
      return null;
    }

    return Collections.unmodifiableCollection(new ArrayList<T>(collection));
  }

  @SuppressWarnings("unchecked")
  private <T> void fireCacheConfigurationChange(CacheConfigurationProperty prop, final T oldValue, final T newValue) {
    if ((oldValue != null && !oldValue.equals(newValue)) || newValue != null) {
      for (CacheConfigurationChangeListener cacheConfigurationListener : cacheConfigurationListenerList) {
        cacheConfigurationListener.cacheConfigurationChange(new CacheConfigurationChangeEvent(prop, oldValue, newValue));
      }
    }
  }
}