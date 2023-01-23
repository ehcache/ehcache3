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

package org.ehcache.impl.internal.events;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/**
 * @author Ludovic Orban
 */
public class DisabledCacheEventNotificationService<K, V> implements CacheEventDispatcher<K, V> {

  @Override
  public void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering, EventFiring firing, EnumSet<EventType> eventTypes) {
  }

  @Override
  public void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
  }

  @Override
  public void shutdown() {
  }

  @Override
  public void setListenerSource(Cache<K, V> source) {
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    return Collections.emptyList();
  }

  @Override
  public void setStoreEventSource(StoreEventSource<K, V> eventSource) {
  }
}
