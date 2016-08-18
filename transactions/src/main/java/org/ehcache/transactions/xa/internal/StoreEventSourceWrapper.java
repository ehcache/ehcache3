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

package org.ehcache.transactions.xa.internal;

import org.ehcache.event.EventType;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.internal.events.StoreEventImpl;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.core.spi.store.events.StoreEventSource;

import java.util.Map;

import static org.ehcache.impl.internal.events.StoreEvents.createEvent;
import static org.ehcache.impl.internal.events.StoreEvents.updateEvent;

/**
 * StoreEventSourceWrapper
 */
class StoreEventSourceWrapper<K, V> implements StoreEventSource<K, V> {

  private final StoreEventSource<K, SoftLock<V>> underlying;
  private final Map<StoreEventListener<K, V>, StoreEventListener<K, SoftLock<V>>> listenersMap = new ConcurrentHashMap<StoreEventListener<K, V>, StoreEventListener<K, SoftLock<V>>>(10);

  StoreEventSourceWrapper(StoreEventSource<K, SoftLock<V>> underlying) {
    this.underlying = underlying;
    underlying.addEventFilter(new StoreEventFilter<K, SoftLock<V>>() {
      @Override
      public boolean acceptEvent(EventType type, K key, SoftLock<V> oldValue, SoftLock<V> newValue) {
        if (newValue != null) {
          return newValue.getOldValue() != null;
        } else if (oldValue != null) {
          return oldValue.getOldValue() != null;
        }
        return false;
      }
    });
  }

  @Override
  public void addEventListener(final StoreEventListener<K, V> eventListener) {
    StoreEventListenerWrapper<K, V> listenerWrapper = new StoreEventListenerWrapper<K, V>(eventListener);
    listenersMap.put(eventListener, listenerWrapper);
    underlying.addEventListener(listenerWrapper);
  }

  @Override
  public void removeEventListener(StoreEventListener<K, V> eventListener) {
    StoreEventListener<K, SoftLock<V>> listenerWrapper = listenersMap.get(eventListener);
    if (listenerWrapper != null) {
      underlying.removeEventListener(listenerWrapper);
    }
  }

  @Override
  public void addEventFilter(final StoreEventFilter<K, V> eventFilter) {
    underlying.addEventFilter(new StoreEventFilter<K, SoftLock<V>>() {
      @Override
      public boolean acceptEvent(EventType type, K key, SoftLock<V> oldValue, SoftLock<V> newValue) {
        V unwrappedOldValue = null;
        V unwrappedNewValue = null;
        if (oldValue != null) {
          unwrappedOldValue = oldValue.getOldValue();
        }
        if (newValue != null) {
          unwrappedNewValue = newValue.getOldValue();
        }
        if (unwrappedNewValue == null && unwrappedOldValue == null) {
          return false;
        }
        return eventFilter.acceptEvent(type, key, unwrappedOldValue, unwrappedNewValue);
      }
    });
  }

  @Override
  public void setEventOrdering(boolean ordering) {
    underlying.setEventOrdering(ordering);
  }

  @Override
  public boolean isEventOrdering() {
    return underlying.isEventOrdering();
  }

  private static class StoreEventListenerWrapper<K, V> implements StoreEventListener<K, SoftLock<V>> {

    private final StoreEventListener<K, V> wrappedOne;

    private StoreEventListenerWrapper(StoreEventListener<K, V> wrappedOne) {
      if (wrappedOne == null) {
        throw new NullPointerException("Wrapped StoreEventListener cannot be null");
      }
      this.wrappedOne = wrappedOne;
    }

    @Override
    public void onEvent(StoreEvent<K, SoftLock<V>> event) {
      StoreEvent<K, V> eventToPropagate = null;
      switch (event.getType()) {
        case CREATED:
          eventToPropagate = createEvent(event.getKey(), event.getNewValue().getOldValue());
          break;
        case UPDATED:
          eventToPropagate = updateEvent(event.getKey(), event.getOldValue().getOldValue(), event.getNewValue()
                .getOldValue());
          break;
        case REMOVED:
        case EXPIRED:
        case EVICTED:
          eventToPropagate = new StoreEventImpl<K, V>(event.getType(), event.getKey(), event.getOldValue().getOldValue(), null);
          break;
      }
      wrappedOne.onEvent(eventToPropagate);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      StoreEventListenerWrapper<?, ?> that = (StoreEventListenerWrapper<?, ?>) o;

      return wrappedOne.equals(that.wrappedOne);

    }

    @Override
    public int hashCode() {
      return wrappedOne.hashCode();
    }
  }
}
