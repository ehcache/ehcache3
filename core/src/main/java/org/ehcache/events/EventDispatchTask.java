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

import org.ehcache.event.CacheEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author rism
 */
public class EventDispatchTask<K, V> implements Runnable {
  private static final Logger LOGGER = LoggerFactory.getLogger(EventDispatchTask.class);
  private final CacheEventWrapper<K, V> cacheEventWrapper;
  private final Iterable<EventListenerWrapper> listeners;

  EventDispatchTask(CacheEventWrapper<K, V> cacheEventWrapper, Iterable<EventListenerWrapper> listener) {
    this.cacheEventWrapper = cacheEventWrapper;
    this.listeners = listener;
  }

  @Override
  public void run() {
    for(EventListenerWrapper listener : listeners) {
      if (!listener.config.fireOn().contains(cacheEventWrapper.cacheEvent.getType())) {
        continue;
      }
      try {
        listener.getListener().onEvent((CacheEvent<Object, Object>)cacheEventWrapper.cacheEvent);
      } catch (Exception e) {
        LOGGER.warn(listener.getListener() + " Failed to fire Event due to ", e);
      }
    }
  }
}