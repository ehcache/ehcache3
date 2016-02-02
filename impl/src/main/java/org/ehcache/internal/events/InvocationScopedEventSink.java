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

package org.ehcache.internal.events;

import org.ehcache.event.EventType;
import org.ehcache.spi.cache.events.StoreEventFilter;
import org.ehcache.spi.cache.events.StoreEventListener;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import static org.ehcache.internal.events.StoreEvents.createEvent;
import static org.ehcache.internal.events.StoreEvents.evictEvent;
import static org.ehcache.internal.events.StoreEvents.expireEvent;
import static org.ehcache.internal.events.StoreEvents.removeEvent;
import static org.ehcache.internal.events.StoreEvents.updateEvent;

/**
 * InvocationScopedEventSink
 */
class InvocationScopedEventSink<K, V> implements CloseableStoreEventSink<K, V> {

  private final Set<StoreEventFilter<K, V>> filters;
  private final boolean ordered;
  private final BlockingQueue<FireableStoreEventHolder<K, V>>[] orderedQueues;
  private final Set<StoreEventListener<K, V>> listeners;
  private final Deque<FireableStoreEventHolder<K, V>> events = new ArrayDeque<FireableStoreEventHolder<K, V>>(4);

  InvocationScopedEventSink(Set<StoreEventFilter<K, V>> filters, boolean ordered,
                            BlockingQueue<FireableStoreEventHolder<K, V>>[] orderedQueues,
                            Set<StoreEventListener<K, V>> listeners) {
    this.filters = filters;
    this.ordered = ordered;
    this.orderedQueues = orderedQueues;
    this.listeners = listeners;
  }

  @Override
  public void removed(K key, V value) {
    if (acceptEvent(EventType.REMOVED, key, value, null)) {
      handleEvent(key, new FireableStoreEventHolder<K, V>(removeEvent(key, value)));
    }
  }

  @Override
  public void updated(K key, V oldValue, V newValue) {
    if (acceptEvent(EventType.UPDATED, key, oldValue, newValue)) {
      handleEvent(key, new FireableStoreEventHolder<K, V>(updateEvent(key, oldValue, newValue)));
    }
  }

  @Override
  public void expired(K key, V value) {
    if (acceptEvent(EventType.EXPIRED, key, value, null)) {
      handleEvent(key, new FireableStoreEventHolder<K, V>(expireEvent(key, value)));
    }
  }

  @Override
  public void created(K key, V value) {
    if (acceptEvent(EventType.CREATED, key, null, value)) {
      handleEvent(key, new FireableStoreEventHolder<K, V>(createEvent(key, value)));
    }
  }

  @Override
  public void evicted(K key, V value) {
    if (acceptEvent(EventType.EVICTED, key, value, null)) {
      handleEvent(key, new FireableStoreEventHolder<K, V>(evictEvent(key, value)));
    }
  }

  private boolean acceptEvent(EventType type, K key, V oldValue, V newValue) {
    for (StoreEventFilter<K, V> filter : filters) {
      if (!filter.acceptEvent(type, key, oldValue, newValue)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void close() {
    if (ordered) {
      fireOrdered(listeners, events);
    } else {
      for (FireableStoreEventHolder<K, V> fireableEvent : events) {
        for (StoreEventListener<K, V> listener : listeners) {
          fireableEvent.fireOn(listener);
        }
      }
    }
  }

  @Override
  public void closeOnFailure() {
    for (FireableStoreEventHolder<K, V> fireableEvent : events) {
      fireableEvent.markFailed();
    }

    close();
  }

  private BlockingQueue<FireableStoreEventHolder<K, V>> getOrderedQueue(FireableStoreEventHolder<K, V> event) {
    return orderedQueues[event.eventKeyHash() % orderedQueues.length];
  }

  private void handleEvent(K key, FireableStoreEventHolder<K, V> event) {
    events.add(event);
    if (ordered) {
      try {
        getOrderedQueue(event).put(event);
      } catch (InterruptedException e) {
        events.removeLast();
        Thread.currentThread().interrupt();
      }
    }
  }

  private void fireOrdered(Set<StoreEventListener<K, V>> listeners, Deque<FireableStoreEventHolder<K, V>> events) {
    for (FireableStoreEventHolder<K, V> fireableEvent : events) {
      fireableEvent.markFireable();

      BlockingQueue<FireableStoreEventHolder<K, V>> orderedQueue = getOrderedQueue(fireableEvent);
      FireableStoreEventHolder<K, V> head = orderedQueue.peek();
      if (head == fireableEvent) {
        // Need to fire my event, plus any it was blocking
        do {
          if (head.markFired()) {
            // Only proceed if I am the one marking fired
            // Do not notify failed events
            for (StoreEventListener<K, V> listener : listeners) {
              head.fireOn(listener);
            }
            orderedQueue.poll(); // Remove the event I just handled
          } else {
            // Someone else fired it - stopping there
            if (head == fireableEvent) {
              // Lost the fire race - may need to wait for full processing
              fireableEvent.waitTillFired();
            }
            break;
          }
        } while ((head = orderedQueue.peek()) != null && head.isFireable());
      } else {
        // Waiting for another thread to fire - once that happens, done for this event
        fireableEvent.waitTillFired();
      }
    }
  }
}
