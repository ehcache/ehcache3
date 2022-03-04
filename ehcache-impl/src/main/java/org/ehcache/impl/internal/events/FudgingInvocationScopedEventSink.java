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
import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * This class is responsible for handling the event fudging that needs to happen
 * in AbstractOffHeapStore because eviction event is fired after create / update
 * events have been recorded. But in reality, eviction happened first as the memory
 * limit on offheap is hard.
 * <p>
 * This creates a special case where we have to rewrite events in case we get an evicted
 * event on the same key than the previous non eviction event. In that case we need to get rid
 * of that former event (in case of UPDATED) or former events (EXPIRED followed by CREATED) and
 * add a CREATED event after the EXPIRED one.
 */
class FudgingInvocationScopedEventSink<K, V> extends InvocationScopedEventSink<K, V> {

  FudgingInvocationScopedEventSink(Set<StoreEventFilter<K, V>> filters, boolean ordered,
                                   BlockingQueue<FireableStoreEventHolder<K, V>>[] orderedQueues,
                                   Set<StoreEventListener<K, V>> listeners) {
    super(filters, ordered, orderedQueues, listeners);
  }

  @Override
  public void evicted(K key, Supplier<V> value) {
    V eventFudgingValue = handleEvictionPostWriteOnSameKey(key);
    super.evicted(key, value);
    if (eventFudgingValue != null) {
      created(key, eventFudgingValue);
    }
  }

  private V handleEvictionPostWriteOnSameKey(K key) {
      Iterator<FireableStoreEventHolder<K, V>> iterator = getEvents().descendingIterator();
      while (iterator.hasNext()) {
        FireableStoreEventHolder<K, V> eventHolder = iterator.next();
        if (eventHolder.getEvent().getType() != EventType.EVICTED) {
          if (eventHolder.getEvent().getKey().equals(key)) {
            // Found the previous non eviction event
            switch (eventHolder.getEvent().getType()) {
              case UPDATED:
                eventHolder.markFailed();
                return eventHolder.getEvent().getNewValue();
              case CREATED:
                eventHolder.markFailed();
                if (iterator.hasNext()) { // Expecting previous event to be EXPIRY
                  FireableStoreEventHolder<K, V> next = iterator.next();
                  if (next.getEvent().getType() == EventType.EXPIRED && next.getEvent().getKey().equals(key)) {
                    next.markFailed();
                  }
                }
                return eventHolder.getEvent().getNewValue();
            }
          } else {
            return null;
          }
        }
      }
    return null;
  }
}
