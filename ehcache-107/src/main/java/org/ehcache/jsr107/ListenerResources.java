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

import java.io.Closeable;
import java.util.Collections;
import java.util.List;

import javax.cache.Cache;
import javax.cache.CacheException;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;

import static org.ehcache.jsr107.CloseUtil.closeAllAfter;
import static org.ehcache.jsr107.CloseUtil.closeAll;

/**
 * ListenerResources
 */
class ListenerResources<K, V> implements Closeable {

  private final CacheEntryListener<? super K, ? super V> listener;
  private final CacheEntryEventFilter<? super K, ? super V> filter;
  private List<EventListenerAdaptors.EventListenerAdaptor<K, V>> ehListeners = null;

  @SuppressWarnings("unchecked")
  static <K, V> ListenerResources<K, V> createListenerResources(CacheEntryListenerConfiguration<K, V> listenerConfig) {
    CacheEntryListener<? super K, ? super V> listener = listenerConfig.getCacheEntryListenerFactory().create();

    // create the filter, closing the listener above upon exception
    CacheEntryEventFilter<? super K, ? super V> filter;
    try {
      Factory<CacheEntryEventFilter<? super K, ? super V>> filterFactory = listenerConfig
          .getCacheEntryEventFilterFactory();
      if (filterFactory != null) {
        filter = listenerConfig.getCacheEntryEventFilterFactory().create();
      } else {
        filter = event -> true;
      }
    } catch (Throwable t) {
      throw closeAllAfter(new CacheException(t), listener);
    }

    try {
      return new ListenerResources<>(listener, filter);
    } catch (Throwable t) {
      throw closeAllAfter(new CacheException(t), filter, listener);
    }
  }



  ListenerResources(CacheEntryListener<? super K, ? super V> listener,
                    CacheEntryEventFilter<? super K, ? super V> filter) {
    this.listener = listener;
    this.filter = filter;
  }

  CacheEntryEventFilter<? super K, ? super V> getFilter() {
    return filter;
  }

  CacheEntryListener<? super K, ? super V> getListener() {
    return listener;
  }

  synchronized List<EventListenerAdaptors.EventListenerAdaptor<K, V>> getEhcacheListeners(Cache<K, V> source, boolean requestsOld) {
    if (ehListeners == null) {
      ehListeners = EventListenerAdaptors.ehListenersFor(listener, filter, source, requestsOld);
    }
    return Collections.unmodifiableList(ehListeners);
  }

  @Override
  public void close() {
    try {
      closeAll(listener, filter);
    } catch (Throwable t) {
      throw new CacheException(t);
    }
  }

}
