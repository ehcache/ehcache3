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
package org.ehcache.jsr107;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.event.CacheEntryCreatedListener;
import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryExpiredListener;
import javax.cache.event.CacheEntryRemovedListener;
import javax.cache.event.CacheEntryUpdatedListener;
import javax.cache.event.EventType;

/**
 * @author teck
 */
class EventListenerAdaptors {

  static abstract class EventListenerAdaptor<K, V> implements org.ehcache.event.CacheEventListener<K, V> {
    abstract org.ehcache.event.EventType getEhcacheEventType();
  }

  @SuppressWarnings("unchecked")
  static <K, V> List<EventListenerAdaptor<K, V>> ehListenersFor(CacheEntryEventFilter<? super K, ? super V> listener,
      CacheEntryEventFilter<? super K, ? super V> filter, Cache<K, V> source) {
    List<EventListenerAdaptor<K, V>> rv = new ArrayList<EventListenerAdaptor<K, V>>();

    if (listener instanceof CacheEntryUpdatedListener) {
      rv.add(new UpdatedAdaptor<K, V>(source, (CacheEntryUpdatedListener<K, V>) listener,
          (CacheEntryEventFilter<K, V>) filter));
    }
    if (listener instanceof CacheEntryCreatedListener) {
      rv.add(new CreatedAdaptor<K, V>(source, (CacheEntryCreatedListener<K, V>) listener,
          (CacheEntryEventFilter<K, V>) filter));
    }
    if (listener instanceof CacheEntryRemovedListener) {
      rv.add(new RemovedAdaptor<K, V>(source, (CacheEntryRemovedListener<K, V>) listener,
          (CacheEntryEventFilter<K, V>) filter));
    }
    if (listener instanceof CacheEntryExpiredListener) {
      rv.add(new ExpiredAdaptor<K, V>(source, (CacheEntryExpiredListener<K, V>) listener,
          (CacheEntryEventFilter<K, V>) filter));
    }

    return rv;
  }

  private EventListenerAdaptors() {
    //
  }

  static class UpdatedAdaptor<K, V> extends EventListenerAdaptor<K, V> {

    private final CacheEntryUpdatedListener<K, V> listener;
    private final CacheEntryEventFilter<K, V> filter;
    private final Cache<K, V> source;

    UpdatedAdaptor(Cache<K, V> source, CacheEntryUpdatedListener<K, V> listener, CacheEntryEventFilter<K, V> filter) {
      this.source = source;
      this.listener = listener;
      this.filter = filter;
    }

    @Override
    org.ehcache.event.EventType getEhcacheEventType() {
      return org.ehcache.event.EventType.UPDATED;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onEvent(org.ehcache.event.CacheEvent<K, V> ehEvent) {
      Eh107CacheEntryEvent<K, V> event = new Eh107CacheEntryEvent<K, V>(source, EventType.CREATED, ehEvent);
      if (filter.evaluate(event)) {
        Set<?> events = Collections.singleton(event);
        listener.onUpdated((Iterable<CacheEntryEvent<? extends K, ? extends V>>) events);
      }
    }
  }

  static class RemovedAdaptor<K, V> extends EventListenerAdaptor<K, V> {

    private final CacheEntryRemovedListener<K, V> listener;
    private final CacheEntryEventFilter<K, V> filter;
    private final Cache<K, V> source;

    RemovedAdaptor(Cache<K, V> source, CacheEntryRemovedListener<K, V> listener, CacheEntryEventFilter<K, V> filter) {
      this.source = source;
      this.listener = listener;
      this.filter = filter;
    }

    @Override
    org.ehcache.event.EventType getEhcacheEventType() {
      return org.ehcache.event.EventType.REMOVED;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onEvent(org.ehcache.event.CacheEvent<K, V> ehEvent) {
      Eh107CacheEntryEvent<K, V> event = new Eh107CacheEntryEvent<K, V>(source, EventType.CREATED, ehEvent);
      if (filter.evaluate(event)) {
        Set<?> events = Collections.singleton(event);
        listener.onRemoved((Iterable<CacheEntryEvent<? extends K, ? extends V>>) events);
      }
    }
  }

  static class ExpiredAdaptor<K, V> extends EventListenerAdaptor<K, V> {

    private final CacheEntryExpiredListener<K, V> listener;
    private final CacheEntryEventFilter<K, V> filter;
    private final Cache<K, V> source;

    ExpiredAdaptor(Cache<K, V> source, CacheEntryExpiredListener<K, V> listener, CacheEntryEventFilter<K, V> filter) {
      this.source = source;
      this.listener = listener;
      this.filter = filter;
    }

    @Override
    org.ehcache.event.EventType getEhcacheEventType() {
      return org.ehcache.event.EventType.EXPIRED;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onEvent(org.ehcache.event.CacheEvent<K, V> ehEvent) {
      Eh107CacheEntryEvent<K, V> event = new Eh107CacheEntryEvent<K, V>(source, EventType.CREATED, ehEvent);
      if (filter.evaluate(event)) {
        Set<?> events = Collections.singleton(event);
        listener.onExpired((Iterable<CacheEntryEvent<? extends K, ? extends V>>) events);
      }
    }
  }

  static class CreatedAdaptor<K, V> extends EventListenerAdaptor<K, V> {

    private final CacheEntryCreatedListener<K, V> listener;
    private final CacheEntryEventFilter<K, V> filter;
    private final Cache<K, V> source;

    CreatedAdaptor(Cache<K, V> source, CacheEntryCreatedListener<K, V> listener, CacheEntryEventFilter<K, V> filter) {
      this.source = source;
      this.listener = listener;
      this.filter = filter;
    }

    @Override
    org.ehcache.event.EventType getEhcacheEventType() {
      return org.ehcache.event.EventType.CREATED;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void onEvent(org.ehcache.event.CacheEvent<K, V> ehEvent) {
      Eh107CacheEntryEvent<K, V> event = new Eh107CacheEntryEvent<K, V>(source, EventType.CREATED, ehEvent);
      if (filter.evaluate(event)) {
        Set<?> events = Collections.singleton(event);
        listener.onCreated((Iterable<CacheEntryEvent<? extends K, ? extends V>>) events);
      }
    }
  }
}
