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
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import javax.cache.Cache;
import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;

/**
 * ListenerResources
 */
class ListenerResources<K, V> implements Closeable {

  private final CacheEntryListener<? super K, ? super V> listener;
  private final CacheEntryEventFilter<? super K, ? super V> filter;
  private List<EventListenerAdaptors.EventListenerAdaptor<K, V>> ehListeners = null;

  @SuppressWarnings("unchecked")
  static <K, V> ListenerResources<K, V> createListenerResources(CacheEntryListenerConfiguration<K, V> listenerConfig,
      MultiCacheException mce) {
    CacheEntryListener<? super K, ? super V> listener = listenerConfig.getCacheEntryListenerFactory().create();

    // create the filter, closing the listener above upon exception
    CacheEntryEventFilter<? super K, ? super V> filter;
    try {
      Factory<CacheEntryEventFilter<? super K, ? super V>> filterFactory = listenerConfig
          .getCacheEntryEventFilterFactory();
      if (filterFactory != null) {
        filter = listenerConfig.getCacheEntryEventFilterFactory().create();
      } else {
        filter = (CacheEntryEventFilter<? super K, ? super V>) NullCacheEntryEventFilter.INSTANCE;
      }
    } catch (Throwable t) {
      mce.addThrowable(t);
      CacheResources.close(listener, mce);
      throw mce;
    }

    try {
      return new ListenerResources<K, V>(listener, filter);
    } catch (Throwable t) {
      mce.addThrowable(t);
      CacheResources.close(filter, mce);
      CacheResources.close(listener, mce);
      throw mce;
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
  public void close() throws IOException {
    MultiCacheException mce = new MultiCacheException();
    CacheResources.close(listener, mce);
    CacheResources.close(filter, mce);
    mce.throwIfNotEmpty();
  }

}
