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

package org.ehcache.events;

import org.ehcache.Cache;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.EnumSet;

/**
 * @author Ludovic Orban
 */
public class DisabledCacheEventNotificationService<K, V> implements CacheEventNotificationService<K, V> {

  @Override
  public void onEvent(CacheEvent<K, V> kvCacheEvent) {
  }

  @Override
  public void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering, EventFiring firing, EnumSet<EventType> eventTypes) {
  }

  @Override
  public boolean hasListeners() {
    return false;
  }

  @Override
  public void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
  }

  @Override
  public void releaseAllListeners() {
  }

  @Override
  public void setStoreListenerSource(Cache<K, V> source) {
  }
}
