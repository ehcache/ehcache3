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

package org.ehcache.core.events;

import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;

import java.util.function.Supplier;

/**
 * NullStoreEventDispatcher
 */
public class NullStoreEventDispatcher<K, V> implements StoreEventDispatcher<K, V> {

  public static <K, V> StoreEventDispatcher<K, V> nullStoreEventDispatcher() {
    return new NullStoreEventDispatcher<>();
  }

  private final StoreEventSink<K, V> storeEventSink = new StoreEventSink<K, V>() {
    @Override
    public void evicted(K key, Supplier<V> value) {
      // Do nothing
    }

    @Override
    public void expired(K key, Supplier<V> value) {
      // Do nothing
    }

    @Override
    public void created(K key, V value) {
      // Do nothing
    }

    @Override
    public void updated(K key, Supplier<V> previousValue, V newValue) {
      // Do nothing
    }

    @Override
    public void removed(K key, Supplier<V> removed) {
      // Do nothing
    }
  };

  @Override
  public StoreEventSink<K, V> eventSink() {
    return storeEventSink;
  }

  @Override
  public void releaseEventSink(StoreEventSink<K, V> eventSink) {
    // Do nothing
  }

  @Override
  public void releaseEventSinkAfterFailure(StoreEventSink<K, V> eventSink, Throwable throwable) {
    // Do nothing
  }

  @Override
  public void reset(StoreEventSink<K, V> eventSink) {
    // Do nothing
  }

  @Override
  public void addEventListener(StoreEventListener<K, V> eventListener) {
    // Do nothing
  }

  @Override
  public void removeEventListener(StoreEventListener<K, V> eventListener) {
    // Do nothing
  }

  @Override
  public void addEventFilter(StoreEventFilter<K, V> eventFilter) {
    // Do nothing
  }

  @Override
  public void setEventOrdering(boolean ordering) {
    // Do nothing
  }

  @Override
  public void setSynchronous(boolean synchronous) {
    // Do nothing
  }

  @Override
  public boolean isEventOrdering() {
    return false;
  }
}
