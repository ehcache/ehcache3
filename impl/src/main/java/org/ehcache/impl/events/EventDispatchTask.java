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

package org.ehcache.impl.events;

import org.ehcache.core.events.EventListenerWrapper;
import org.ehcache.event.CacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class EventDispatchTask<K, V> implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventDispatchTask.class);
  private final CacheEvent<K, V> cacheEvent;
  private final Iterable<EventListenerWrapper<K, V>> listenerWrappers;

  EventDispatchTask(CacheEvent<K, V> cacheEvent, Iterable<EventListenerWrapper<K, V>> listener) {
    if (cacheEvent == null) {
      throw new NullPointerException("cache event cannot be null");
    }
    if (listener == null) {
      throw new NullPointerException("listener cannot be null");
    }
    this.cacheEvent = cacheEvent;
    this.listenerWrappers = listener;
  }

  @Override
  public void run() {
    for(EventListenerWrapper<K, V> listenerWrapper : listenerWrappers) {
      if (listenerWrapper.isForEventType(cacheEvent.getType())) {
        try {
          listenerWrapper.onEvent(cacheEvent);
        } catch (Exception e) {
          LOGGER.warn(listenerWrapper.getListener() + " Failed to fire Event due to ", e);
        }
      }
    }
  }
}
