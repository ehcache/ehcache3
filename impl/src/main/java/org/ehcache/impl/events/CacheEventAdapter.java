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

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;

/**
 * Adapter for cache event listener implementation that splits the event based on its type
 * to specific methods. This class by default provides only empty methods.
 *
 * @param <K> the type of the keys from the cache
 * @param <V> the type of the values from the cache
 */
public abstract class CacheEventAdapter<K, V> implements CacheEventListener<K, V> {

  @Override
  public final void onEvent(CacheEvent<K, V> event) {
    switch (event.getType()) {
      case CREATED:
        onCreation(event.getKey(), event.getNewValue());
        break;
      case UPDATED:
        onUpdate(event.getKey(), event.getOldValue(), event.getNewValue());
        break;
      case REMOVED:
        onRemoval(event.getKey(), event.getOldValue());
        break;
      case EXPIRED:
        onExpiry(event.getKey(), event.getOldValue());
        break;
      case EVICTED:
        onEviction(event.getKey(), event.getOldValue());
        break;
      default:
        throw new AssertionError("Unsupported event type " + event.getType());
    }
  }

  protected void onEviction(K key, V evictedValue) {
    // Do nothing by default
  }

  protected void onExpiry(K key, V expiredValue) {
    // Do nothing by default
  }

  protected void onRemoval(K key, V removedValue) {
    // Do nothing by default
  }

  protected void onUpdate(K key, V oldValue, V newValue) {
    // Do nothing by default
  }

  protected void onCreation(K key, V newValue) {
    // Do nothing by default
  }
}
