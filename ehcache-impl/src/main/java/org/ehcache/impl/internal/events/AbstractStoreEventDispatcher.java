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

import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

/**
 * AbstractStoreEventDispatcher
 */
public abstract class AbstractStoreEventDispatcher<K, V> implements StoreEventDispatcher<K, V> {

  protected static final StoreEventSink<?, ?> NO_OP_EVENT_SINK = new CloseableStoreEventSink<Object, Object>() {
    @Override
    public void close() {
      // Do nothing
    }

    @Override
    public void closeOnFailure() {
      // Do nothing
    }

    @Override
    public void reset() {
      // Do nothing
    }

    @Override
    public void removed(Object key, Supplier<Object> value) {
      // Do nothing
    }

    @Override
    public void updated(Object key, Supplier<Object> oldValue, Object newValue) {
      // Do nothing
    }

    @Override
    public void expired(Object key, Supplier<Object> value) {
      // Do nothing
    }

    @Override
    public void created(Object key, Object value) {
      // Do nothing
    }

    @Override
    public void evicted(Object key, Supplier<Object> value) {
      // Do nothing
    }
  };

  private final Set<StoreEventFilter<K, V>> filters = new CopyOnWriteArraySet<>();
  private final Set<StoreEventListener<K, V>> listeners = new CopyOnWriteArraySet<>();
  private final BlockingQueue<FireableStoreEventHolder<K, V>>[] orderedQueues;
  private volatile boolean ordered = false;

  protected AbstractStoreEventDispatcher(int dispatcherConcurrency) {
    if (dispatcherConcurrency <= 0) {
      throw new IllegalArgumentException("Dispatcher concurrency must be an integer greater than 0");
    }
    @SuppressWarnings("unchecked")
    LinkedBlockingQueue<FireableStoreEventHolder<K, V>>[] queues = (LinkedBlockingQueue<FireableStoreEventHolder<K, V>>[])
      new LinkedBlockingQueue<?>[dispatcherConcurrency];
    orderedQueues = queues;
    for (int i = 0; i < orderedQueues.length; i++) {
      orderedQueues[i] = new LinkedBlockingQueue<>(10000);
    }
  }

  protected Set<StoreEventListener<K, V>> getListeners() {
    return listeners;
  }

  protected Set<StoreEventFilter<K, V>> getFilters() {
    return filters;
  }

  protected BlockingQueue<FireableStoreEventHolder<K, V>>[] getOrderedQueues() {
    return orderedQueues;
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
  public void setSynchronous(boolean synchronous) throws IllegalArgumentException {
    //dispatcher is synchronous by default
  }

  @Override
  public boolean isEventOrdering() {
    return ordered;
  }

  @Override
  public void releaseEventSink(StoreEventSink<K, V> eventSink) {
    ((CloseableStoreEventSink) eventSink).close();
  }

  @Override
  public void releaseEventSinkAfterFailure(StoreEventSink<K, V> eventSink, Throwable throwable) {
    ((CloseableStoreEventSink) eventSink).closeOnFailure();
  }

  @Override
  public void reset(StoreEventSink<K, V> eventSink) {
    ((CloseableStoreEventSink) eventSink).reset();
  }

  @Override
  public StoreEventSink<K, V> eventSink() {
    return new InvocationScopedEventSink<>(getFilters(), isEventOrdering(), getOrderedQueues(), getListeners());
  }
}
