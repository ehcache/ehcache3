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
import org.ehcache.events.StoreEventDispatcher;
import org.ehcache.events.StoreEventSink;
import org.ehcache.spi.cache.events.StoreEvent;
import org.ehcache.spi.cache.events.StoreEventFilter;
import org.ehcache.spi.cache.events.StoreEventListener;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.ehcache.internal.events.StoreEvents.createEvent;
import static org.ehcache.internal.events.StoreEvents.evictEvent;
import static org.ehcache.internal.events.StoreEvents.expireEvent;
import static org.ehcache.internal.events.StoreEvents.removeEvent;
import static org.ehcache.internal.events.StoreEvents.updateEvent;

/**
 * ScopedStoreEventDispatcher
 */
public class ScopedStoreEventDispatcher<K, V> implements StoreEventDispatcher<K, V> {

  private static final StoreEventSink NO_OP_EVENT_SINK = new StoreEventSink() {
    @Override
    public void removed(Object key, Object value) {
      // Do nothing
    }

    @Override
    public void updated(Object key, Object oldValue, Object newValue) {
      // Do nothing
    }

    @Override
    public void expired(Object key, Object value) {
      // Do nothing
    }

    @Override
    public void created(Object key, Object value) {
      // Do nothing
    }

    @Override
    public void evicted(Object key, Object value) {
      // Do nothing
    }
  };

  private final Set<StoreEventFilter<K, V>> filters = new CopyOnWriteArraySet<StoreEventFilter<K, V>>();
  private final Set<StoreEventListener<K, V>> listeners = new CopyOnWriteArraySet<StoreEventListener<K, V>>();
  private final BlockingQueue<FireableStoreEventWrapper<K, V>>[] orderedQueues;
  private volatile boolean ordered = false;

  public ScopedStoreEventDispatcher(int orderedEventParallelism) {
    if (orderedEventParallelism <= 0) {
      throw new IllegalArgumentException("Ordered event parallelism must be an integer greater than 0");
    }
    orderedQueues = new LinkedBlockingQueue[orderedEventParallelism];
    for (int i = 0; i < orderedQueues.length; i++) {
      orderedQueues[i] = new LinkedBlockingQueue<FireableStoreEventWrapper<K, V>>(10000);
    }
  }

  @Override
  public StoreEventSink<K, V> eventSink() {
    if (listeners.isEmpty()) {
      return NO_OP_EVENT_SINK;
    } else {
      return new InvocationScopedEventSink<K, V>(filters, ordered, orderedQueues);
    }
  }

  @Override
  public void releaseEventSink(StoreEventSink<K, V> eventSink) {
    if (eventSink != NO_OP_EVENT_SINK) {
      Set<StoreEventListener<K, V>> listeners = this.listeners;
      InvocationScopedEventSink<K, V> sink = (InvocationScopedEventSink<K, V>) eventSink;
      boolean ordered = sink.ordered;
      Deque<FireableStoreEventWrapper<K, V>> events = sink.events;

      if (ordered) {
        fireOrdered(listeners, events);
      } else {
        for (FireableStoreEventWrapper<K, V> fireableEvent : events) {
          for (StoreEventListener<K, V> listener : listeners) {
            listener.onEvent(fireableEvent.event);
          }
        }
      }
    }
  }

  private void fireOrdered(Set<StoreEventListener<K, V>> listeners, Deque<FireableStoreEventWrapper<K, V>> events) {
    for (FireableStoreEventWrapper<K, V> fireableEvent : events) {
      fireableEvent.markFireable();
      BlockingQueue<FireableStoreEventWrapper<K, V>> orderedQueue = orderedQueues[fireableEvent.event.getKey()
                                                                                      .hashCode() % orderedQueues.length];
      FireableStoreEventWrapper<K, V> head = orderedQueue.peek();
      if (head != fireableEvent) {
        // Waiting for another thread to fire - once that happens, done for this event
        fireableEvent.waitTillFired();
        continue;
      }
      // Need to fire my event, plus any it was blocking
      while (head != null && head.fireable.get()) {
        if (head.markFired()) {
          // Only proceed if I am the one marking fired
          if (!head.failed.get()) {
            // Do not notify failed events
            for (StoreEventListener<K, V> listener : listeners) {
              listener.onEvent(head.event);
            }
          }
          orderedQueue.poll(); // Remove the event I just handled
          head = orderedQueue.peek(); // Look at next one
        } else {
          // Someone else fired it - stopping there
          break;
        }
      }
    }
  }

  @Override
  public void releaseEventSinkAfterFailure(StoreEventSink<K, V> eventSink, Throwable throwable) {
    if (eventSink != NO_OP_EVENT_SINK) {
      InvocationScopedEventSink<K, V> sink = (InvocationScopedEventSink) eventSink;

      Deque<FireableStoreEventWrapper<K, V>> events = sink.events;

      for (FireableStoreEventWrapper<K, V> fireableEvent : events) {
        fireableEvent.markFailed();
      }

      // TODO indicate that we are dropping events mostly
      releaseEventSink(eventSink);
    }
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
    this.ordered = ordering;
  }

  @Override
  public boolean isEventOrdering() {
    return ordered;
  }

  private static class InvocationScopedEventSink<K, V> implements StoreEventSink<K, V> {

    private final Set<StoreEventFilter<K, V>> filters;
    private final boolean ordered;
    private final BlockingQueue<FireableStoreEventWrapper<K, V>>[] orderedQueues;
    private final Deque<FireableStoreEventWrapper<K, V>> events = new ArrayDeque<FireableStoreEventWrapper<K, V>>(4);

    private InvocationScopedEventSink(Set<StoreEventFilter<K, V>> filters,
                                      boolean ordered, BlockingQueue<FireableStoreEventWrapper<K, V>>[] orderedQueues) {
      this.filters = filters;
      this.ordered = ordered;
      this.orderedQueues = orderedQueues;
    }

    @Override
    public void removed(K key, V value) {
      if (acceptEvent(EventType.REMOVED, key, value, null)) {
        handleEvent(key, new FireableStoreEventWrapper<K, V>(removeEvent(key, value)));
      }
    }

    @Override
    public void updated(K key, V oldValue, V newValue) {
      if (acceptEvent(EventType.UPDATED, key, oldValue, newValue)) {
        handleEvent(key, new FireableStoreEventWrapper<K, V>(updateEvent(key, oldValue, newValue)));
      }
    }

    @Override
    public void expired(K key, V value) {
      if (acceptEvent(EventType.EXPIRED, key, value, null)) {
        handleEvent(key, new FireableStoreEventWrapper<K, V>(expireEvent(key, value)));
      }
    }

    @Override
    public void created(K key, V value) {
      if (acceptEvent(EventType.CREATED, key, null, value)) {
        handleEvent(key, new FireableStoreEventWrapper<K, V>(createEvent(key, value)));
      }
    }

    @Override
    public void evicted(K key, V value) {
      if (acceptEvent(EventType.EVICTED, key, value, null)) {
        handleEvent(key, new FireableStoreEventWrapper<K, V>(evictEvent(key, value)));
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

    private void handleEvent(K key, FireableStoreEventWrapper<K, V> event) {
      events.add(event);
      if (ordered) {
        try {
          orderedQueues[key.hashCode() % orderedQueues.length].put(event);
        } catch (InterruptedException e) {
          events.removeLast();
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private static class FireableStoreEventWrapper<K, V> {
    private final Lock lock = new ReentrantLock();
    private final AtomicBoolean fireable = new AtomicBoolean(false);
    private final AtomicBoolean fired = new AtomicBoolean(false);
    private final AtomicBoolean failed = new AtomicBoolean(false);

    private final StoreEvent<K, V> event;
    private final Condition condition;

    private FireableStoreEventWrapper(StoreEvent<K, V> event) {
      this.event = event;
      this.condition = lock.newCondition();
    }

    void markFireable() {
      fireable.set(true);
    }

    void waitTillFired() {
      int interruptCount = 0;
      while (!fired.get()) {
        lock.lock();
        try {
          if (!fired.get()) {
            condition.await();
          }
        } catch (InterruptedException e) {
          interruptCount++;
          if (interruptCount >= 5) {
            // TODO decide what happens??
          }
        } finally {
          lock.unlock();
        }
      }
    }

    boolean markFired() {
      boolean didIt = fired.compareAndSet(false, true);
      if (didIt) {
        lock.lock();
        try {
          condition.signal();
        } finally {
          lock.unlock();
        }
      }
      return didIt;
    }

    void markFailed() {
      failed.set(true);
    }
  }

}
