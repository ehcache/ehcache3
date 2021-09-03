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

package org.ehcache.core;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.core.events.EventListenerWrapper;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

class EhcacheRuntimeConfiguration<K, V> implements CacheRuntimeConfiguration<K, V>, InternalRuntimeConfiguration, HumanReadable {

  private final Collection<ServiceConfiguration<?>> serviceConfigurations;
  private final CacheConfiguration<? super K, ? super V> config;
  private final Class<K> keyType;
  private final Class<V> valueType;
  private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final ClassLoader classLoader;
  private final ExpiryPolicy<? super K, ? super V> expiry;
  private volatile ResourcePools resourcePools;

  private final List<CacheConfigurationChangeListener> cacheConfigurationListenerList
      = new CopyOnWriteArrayList<>();

  EhcacheRuntimeConfiguration(CacheConfiguration<K, V> config) {
    this.config = config;
    this.serviceConfigurations = copy(config.getServiceConfigurations());
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.evictionAdvisor = config.getEvictionAdvisor();
    this.classLoader = config.getClassLoader();
    this.expiry = config.getExpiryPolicy();
    this.resourcePools = config.getResourcePools();
  }

  @Override
  public synchronized void updateResourcePools(ResourcePools pools) {

    if(pools == null) {
      throw new NullPointerException("Pools to be updated cannot be null");
    }

    ResourcePools updatedResourcePools = config.getResourcePools().validateAndMerge(pools);
    fireCacheConfigurationChange(CacheConfigurationProperty.UPDATE_SIZE, config.getResourcePools(), updatedResourcePools);
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
  public EvictionAdvisor<? super K, ? super V> getEvictionAdvisor() {
    return this.evictionAdvisor;
  }

  @Override
  public ClassLoader getClassLoader() {
    return this.classLoader;
  }

  @SuppressWarnings("deprecation")
  @Override
  public org.ehcache.expiry.Expiry<? super K, ? super V> getExpiry() {
    return ExpiryUtils.convertToExpiry(expiry);
  }

  @Override
  public ExpiryPolicy<? super K, ? super V> getExpiryPolicy() {
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
    fireCacheConfigurationChange(CacheConfigurationProperty.REMOVE_LISTENER, listener, listener);
  }

  @Override
  public synchronized void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering,
                                                      EventFiring firing, Set<EventType> forEventTypes) {
    EventListenerWrapper<K, V> listenerWrapper = new EventListenerWrapper<>(listener, firing, ordering, EnumSet.copyOf(forEventTypes));
    fireCacheConfigurationChange(CacheConfigurationProperty.ADD_LISTENER, listenerWrapper, listenerWrapper);
  }

  @Override
  public void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering, EventFiring firing, EventType eventType, EventType... eventTypes) {
    EventListenerWrapper<K, V> listenerWrapper = new EventListenerWrapper<>(listener, firing, ordering, EnumSet.of(eventType, eventTypes));
    fireCacheConfigurationChange(CacheConfigurationProperty.ADD_LISTENER, listenerWrapper, listenerWrapper);
  }

  private <T> Collection<T> copy(Collection<T> collection) {
    if (collection == null) {
      return null;
    }

    return Collections.unmodifiableCollection(new ArrayList<>(collection));
  }

  @SuppressWarnings("unchecked")
  private <T> void fireCacheConfigurationChange(CacheConfigurationProperty prop, final T oldValue, final T newValue) {
    if ((oldValue != null && !oldValue.equals(newValue)) || newValue != null) {
      for (CacheConfigurationChangeListener cacheConfigurationListener : cacheConfigurationListenerList) {
        cacheConfigurationListener.cacheConfigurationChange(new CacheConfigurationChangeEvent(prop, oldValue, newValue));
      }
    }
  }

  @Override
  public String readableString() {
    StringBuilder serviceConfigurationsToStringBuilder = new StringBuilder();
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      serviceConfigurationsToStringBuilder
          .append("\n    ")
          .append("- ");
      if(serviceConfiguration instanceof HumanReadable) {
        serviceConfigurationsToStringBuilder
            .append(((HumanReadable)serviceConfiguration).readableString())
            .append("\n");
      } else {
        serviceConfigurationsToStringBuilder
            .append(serviceConfiguration.getClass().getName())
            .append("\n");
      }
    }

    if(serviceConfigurationsToStringBuilder.length() > 0) {
      serviceConfigurationsToStringBuilder.deleteCharAt(serviceConfigurationsToStringBuilder.length() -1);
    } else {
      serviceConfigurationsToStringBuilder.append(" None");
    }

    String expiryPolicy;

    if (ExpiryPolicy.NO_EXPIRY == expiry) {
      expiryPolicy = "NoExpiryPolicy";
    } else {
      expiryPolicy = expiry.toString();
    }

    return
        "keyType: " + keyType.getName() + "\n" +
        "valueType: " + valueType.getName() + "\n" +
        "serviceConfigurations:" + serviceConfigurationsToStringBuilder.toString().replace("\n", "\n    ") + "\n" +
        "evictionAdvisor: " + ((evictionAdvisor != null) ? evictionAdvisor.getClass().getName() : "None") + "\n" +
        "expiry: " + expiryPolicy + "\n" +
        "resourcePools: " + "\n    " + ((resourcePools instanceof HumanReadable) ? ((HumanReadable)resourcePools).readableString() : "").replace("\n", "\n    ");
  }
}
