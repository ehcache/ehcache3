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
import org.ehcache.config.ResourceType;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.spi.cache.Store;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author rism
 */
public class InternalRuntimeConfigurationImpl<K, V> implements InternalRuntimeConfiguration {

  private CacheEventNotificationService<K, V> eventNotificationService;
  private Store<K, V> store;
  final StoreListener<K, V> storeListener;
  private final List<CacheConfigurationListener> cacheConfigurationListenerList
      = new CopyOnWriteArrayList<CacheConfigurationListener>();
  private Comparable<Long> capacityConstraint;


  public InternalRuntimeConfigurationImpl(CacheConfiguration<K, V> config, Store<K, V> store,
                                          CacheEventNotificationService<K, V> eventNotifier) {

    this.eventNotificationService = eventNotifier;
    this.store = store;
    this.storeListener = new StoreListener<K, V>();
    this.storeListener.setEventNotificationService(eventNotifier);

    TreeSet<ResourceType> resourceTypes = new TreeSet<ResourceType>();
    Collections.addAll(resourceTypes, ResourceType.Core.values());
    Iterator<ResourceType> resourceTypeIterator = resourceTypes.descendingIterator();
    while (resourceTypeIterator.hasNext()) {
      ResourceType currentResourceType = resourceTypeIterator.next();
      if(config.getResourcePools() != null
         && config.getResourcePools().getPoolForResource(currentResourceType) != null) {
        this.capacityConstraint = config.getResourcePools().getPoolForResource(currentResourceType).getSize();
        break;
      }
    }
  }

  public synchronized void setCapacityConstraint(Comparable<Long> constraint) throws UnsupportedOperationException {
    Comparable<Long> oldValue = this.capacityConstraint;
    try {
      this.capacityConstraint = constraint;
      fireCacheConfigurationChange(CacheConfigurationProperties.CAPACITYCONSTRAINT, oldValue, constraint);
    } catch (UnsupportedOperationException uoe) {
      this.capacityConstraint = oldValue;
      throw uoe;
    }
  }

  public void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener,
                                         EventOrdering ordering, EventFiring firing, Set<EventType> forEventTypes) {
    boolean doRegister = forEventTypes.contains(EventType.EVICTED) || forEventTypes.contains(EventType.EXPIRED);
    eventNotificationService.registerCacheEventListener(listener, ordering, firing, EnumSet.copyOf(forEventTypes));
    if (doRegister) {
      store.enableStoreEventNotifications(storeListener);
    }
  }

  public void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
    eventNotificationService.deregisterCacheEventListener(listener);
    if (!eventNotificationService.hasListeners()) {
      store.disableStoreEventNotifications();
    }
  }

  @Override
  public boolean addCacheConfigurationListener(final CacheConfigurationListener listener) {
    return this.cacheConfigurationListenerList.add(listener);
  }

  @Override
  public boolean removeCacheConfigurationListener(final CacheConfigurationListener listener) {
    return this.cacheConfigurationListenerList.remove(listener);
  }

  //TODO: should be removed once work on #346 is complete
  public void releaseAllEventListeners() {
    eventNotificationService.releaseAllListeners();
  }

  private <T> void fireCacheConfigurationChange(CacheConfigurationProperties prop, final T oldValue, final T newValue) {
    if ((oldValue != null && !oldValue.equals(newValue)) || newValue != null) {
      for (CacheConfigurationListener cacheConfigurationListener : cacheConfigurationListenerList) {
        cacheConfigurationListener.cacheConfigurationChange(new CacheConfigurationChangeEvent(prop, oldValue, newValue));
      }
    }
  }
}
