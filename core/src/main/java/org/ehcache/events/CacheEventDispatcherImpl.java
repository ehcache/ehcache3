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

package org.ehcache.events;

import org.ehcache.Cache;
import org.ehcache.CacheConfigurationChangeEvent;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.CacheConfigurationProperty;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.events.StoreEvent;
import org.ehcache.spi.cache.events.StoreEventListener;
import org.ehcache.spi.cache.events.StoreEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-cache component that manages cache event listener registrations, and provides event delivery based on desired
 * firing mode and ordering, for specified event types. 
 * <p>
 * <h5>Note on event ordering guarantees:</h5> {@link #onEvent(CacheEvent)} is assumed to be called within a key-based
 * lock scope. If that is not the case, this facility has no means of maintaining event ordering consistent with source 
 * of such events. That is - listeners registered to receive events in the order they occurred in underlying store may be 
 * invoked in an order inconsistent with actual ordering of corresponding operations on said store.
 * <p>
 * Conversely, sending events to this service inside lock scope, when there are no registered listeners interested in 
 * ordered event delivery is harmless, i.e. event delivery to unordered listeners will still occur.
 */
public class CacheEventDispatcherImpl<K, V> implements CacheEventDispatcher<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheEventDispatcherImpl.class);
  private final ExecutorService unOrderedExectuor;
  private final ExecutorService orderedExecutor;
  private final AtomicInteger listenersCount = new AtomicInteger(0);
  private final AtomicInteger orderedListenerCount = new AtomicInteger(0);
  private final Set<EventListenerWrapper> syncListenersSet = new CopyOnWriteArraySet<EventListenerWrapper>();
  private final Set<EventListenerWrapper> aSyncListenersSet = new CopyOnWriteArraySet<EventListenerWrapper>();
  private final StoreEventSource<K, V> storeEventSource;
  private final StoreEventListener<K, V> eventListener = new StoreListener();

  private Cache<K, V> listenerSource;

  public CacheEventDispatcherImpl(Store<K, V> store, ExecutorService unOrderedExectuor, ExecutorService orderedExecutor) {
    storeEventSource = store.getStoreEventSource();
    this.unOrderedExectuor = unOrderedExectuor;
    this.orderedExecutor = orderedExecutor;
  }

  /**
   * Allows for registering {@link org.ehcache.event.CacheEventListener} on the cache
   *
   * @param listener the listener instance to register
   * @param ordering the {@link org.ehcache.event.EventOrdering} to invoke this listener
   * @param firing the {@link org.ehcache.event.EventFiring} to invoke this listener
   * @param forEventTypes the {@link org.ehcache.event.EventType} to notify this listener of
   *
   * @throws java.lang.IllegalStateException if the listener is already registered
   */
  @Override
  public void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener,
                                  EventOrdering ordering, EventFiring firing, EnumSet<EventType> forEventTypes) {
    EventListenerWrapper wrapper = new EventListenerWrapper(listener, firing, ordering, forEventTypes);

    registerCacheEventListener(wrapper);
  }

  private void registerCacheEventListener(EventListenerWrapper wrapper) {
    if(aSyncListenersSet.contains(wrapper) || syncListenersSet.contains(wrapper)) {
      throw new IllegalStateException("Cache Event Listener already registered: " + wrapper.listener);
    }

    if(wrapper.ordering == EventOrdering.ORDERED && orderedListenerCount.getAndIncrement() == 0) {
      storeEventSource.setEventOrdering(true);
    }

    switch (wrapper.firing) {
      case ASYNCHRONOUS:
        aSyncListenersSet.add(wrapper);
        break;
      case SYNCHRONOUS:
        syncListenersSet.add(wrapper);
        break;
    }

    if (listenersCount.getAndIncrement() == 0) {
      storeEventSource.addEventListener(eventListener);
    }
  }

  /**
   * Allows for deregistering of a previously registered {@link org.ehcache.event.CacheEventListener} instance
   *
   * @param listener the listener to deregister
   *
   * @throws java.lang.IllegalStateException if the listener isn't already registered
   */
  @Override
  public void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
    EventListenerWrapper wrapper = new EventListenerWrapper(listener);

    if (!removeWrapperFromSet(wrapper, aSyncListenersSet)) {
      if (!removeWrapperFromSet(wrapper, syncListenersSet)) {
        throw new IllegalStateException("Unknown cache event listener: " + listener);
      }
    }
  }

  private boolean removeWrapperFromSet(EventListenerWrapper wrapper, Set<EventListenerWrapper> listenersSet) {
    for (EventListenerWrapper listenerWrapper : listenersSet) {
      if(listenerWrapper.equals(wrapper)) {
        listenersSet.remove(listenerWrapper);
        if(listenerWrapper.ordering == EventOrdering.ORDERED) {
          if (orderedListenerCount.decrementAndGet() == 0) {
            storeEventSource.setEventOrdering(false);
          }
        }
        if (listenersCount.decrementAndGet() == 0) {
          storeEventSource.removeEventListener(eventListener);
        }
        return true;
      }
    }
    return false;
  }

  @Override
  public void shutdown() {
    for (EventListenerWrapper listenerWrapper : aSyncListenersSet) {
      removeWrapperFromSet(listenerWrapper, aSyncListenersSet);
    }
    for (EventListenerWrapper listenerWrapper : syncListenersSet) {
      removeWrapperFromSet(listenerWrapper, syncListenersSet);
    }
    orderedExecutor.shutdown();
  }

  @Override
  public void setListenerSource(Cache<K, V> source) {
    this.listenerSource = source;
  }

  public void onEvent(CacheEvent<K, V> event) {
    ExecutorService executor;
    if (storeEventSource.isEventOrdering()) {
      executor= orderedExecutor;
    } else {
      executor = unOrderedExectuor;
    }
    Future<?> future = null;
    if (!syncListenersSet.isEmpty()) {
      future = executor.submit(new EventDispatchTask<K, V>(event, syncListenersSet));
    }
    if (!aSyncListenersSet.isEmpty()) {
      executor.submit(new EventDispatchTask<K, V>(event, aSyncListenersSet));
    }
    if (future != null) {
      try {
        future.get();
      } catch (Exception e) {
        LOGGER.error("Cache Event Listener failed for event type {} due to ", event.getType(), e);
      }
    }
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> configurationChangeListenerList = new ArrayList<CacheConfigurationChangeListener>();
    configurationChangeListenerList.add(new CacheConfigurationChangeListener() {
      @Override
      public void cacheConfigurationChange(final CacheConfigurationChangeEvent event) {
        if (event.getProperty().equals(CacheConfigurationProperty.ADD_LISTENER)) {
          registerCacheEventListener((EventListenerWrapper)event.getNewValue());
        } else if (event.getProperty().equals(CacheConfigurationProperty.REMOVE_LISTENER)) {
          CacheEventListener<? super K, ? super V> oldListener = (CacheEventListener)event.getOldValue();
          deregisterCacheEventListener(oldListener);
        }
      }
    });
    return configurationChangeListenerList;
  }

  private final class StoreListener implements StoreEventListener<K, V> {

    @Override
    public void onEvent(StoreEvent<K, V> event) {
      switch (event.getType()) {
        case CREATED:
          CacheEventDispatcherImpl.this.onEvent(CacheEvents.creation(event.getKey(), event.getNewValue(), listenerSource));
          break;
        case UPDATED:
          CacheEventDispatcherImpl.this.onEvent(CacheEvents.update(event.getKey(), event.getOldValue(), event.getNewValue(), listenerSource));
          break;
        case REMOVED:
          CacheEventDispatcherImpl.this.onEvent(CacheEvents.removal(event.getKey(), event.getOldValue(), listenerSource));
          break;
        case EXPIRED:
          CacheEventDispatcherImpl.this.onEvent(CacheEvents.expiry(event.getKey(), event.getOldValue(), listenerSource));
          break;
        case EVICTED:
          CacheEventDispatcherImpl.this.onEvent(CacheEvents.eviction(event.getKey(), event.getOldValue(), listenerSource));
          break;
      }
    }
  }
}
