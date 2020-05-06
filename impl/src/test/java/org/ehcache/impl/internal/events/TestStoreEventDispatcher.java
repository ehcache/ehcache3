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

import org.ehcache.event.EventType;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

/**
 * TestStoreEventDispatcher
 */
public class TestStoreEventDispatcher<K, V> implements StoreEventDispatcher<K, V> {

  private final Set<StoreEventListener<K, V>> listeners = new HashSet<>(4);
  private final Set<StoreEventFilter<K, V>> filters = new HashSet<>(4);
  private final EventBridge eventBridge = new EventBridge();

  @Override
  public StoreEventSink<K, V> eventSink() {
    return eventBridge;
  }

  private boolean accepted(EventType type, K key, V  oldValue, V newValue) {
    for (StoreEventFilter<K, V> filter : filters) {
      if (!filter.acceptEvent(type, key, oldValue, newValue)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void releaseEventSink(StoreEventSink<K, V> eventSink) {
    // No-op
  }

  @Override
  public void releaseEventSinkAfterFailure(StoreEventSink<K, V> eventSink, Throwable throwable) {
    // No-op
  }

  @Override
  public void reset(StoreEventSink<K, V> eventSink) {
    // No-op
  }

  @Override
  public void addEventListener(StoreEventListener<K, V> eventListener) {
    listeners.add(eventListener);
  }

  @Override
  public void removeEventListener(StoreEventListener<K, V> eventListener) {
    listeners.remove(eventListener);
  }

  @Override
  public void addEventFilter(StoreEventFilter<K, V> eventFilter) {
    filters.add(eventFilter);
  }

  @Override
  public void setEventOrdering(boolean ordering) {
    throw new UnsupportedOperationException("Test impl cannot be made ordered");
  }

  @Override
  public void setSynchronous(boolean synchronous) throws IllegalArgumentException {
    throw new UnsupportedOperationException("Test impl cannot be made synchronous");
  }

  @Override
  public boolean isEventOrdering() {
    return false;
  }

  private class EventBridge implements StoreEventSink<K, V> {
    @Override
    public void evicted(K key, Supplier<V> value) {
      if (accepted(EventType.EVICTED, key, value.get(), null)) {
        StoreEvent<K, V> event = StoreEvents.evictEvent(key, value.get());
        for (StoreEventListener<K, V> listener : listeners) {
          listener.onEvent(event);
        }
      }
    }

    @Override
    public void expired(K key, Supplier<V> value) {
      if (accepted(EventType.EXPIRED, key, value.get(), null)) {
        StoreEvent<K, V> event = StoreEvents.expireEvent(key, value.get());
        for (StoreEventListener<K, V> listener : listeners) {
          listener.onEvent(event);
        }
      }
    }

    @Override
    public void created(K key, V value) {
      if (accepted(EventType.CREATED, key, null, value)) {
        StoreEvent<K, V> event = StoreEvents.createEvent(key, value);
        for (StoreEventListener<K, V> listener : listeners) {
          listener.onEvent(event);
        }
      }
    }

    @Override
    public void updated(K key, Supplier<V> previousValue, V newValue) {
      if (accepted(EventType.UPDATED, key, previousValue.get(), newValue)) {
        StoreEvent<K, V> event = StoreEvents.updateEvent(key, previousValue.get(), newValue);
        for (StoreEventListener<K, V> listener : listeners) {
          listener.onEvent(event);
        }
      }
    }

    @Override
    public void removed(K key, Supplier<V> removed) {
      if (accepted(EventType.REMOVED, key, removed.get(), null)) {
        StoreEvent<K, V> event = StoreEvents.removeEvent(key, removed.get());
        for (StoreEventListener<K, V> listener : listeners) {
          listener.onEvent(event);
        }
      }
    }
  }
}
