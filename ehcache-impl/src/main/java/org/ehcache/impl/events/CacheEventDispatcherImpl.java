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

package org.ehcache.impl.events;

import org.ehcache.Cache;
import org.ehcache.core.CacheConfigurationChangeListener;
import org.ehcache.core.CacheConfigurationProperty;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.events.CacheEvents;
import org.ehcache.core.events.EventListenerWrapper;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableMap;
import static java.util.EnumSet.allOf;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Stream.concat;

/**
 * Per-cache component that manages cache event listener registrations, and provides event delivery based on desired
 * firing mode for specified event types.
 * <p>
 * Use of this class is linked to having cache events on a {@link org.ehcache.UserManagedCache user managed cache}.
 * <p>
 * <em>Note on event ordering guarantees:</em> Events are received and transmitted to register listeners through the
 * registration of a {@link StoreEventListener} on the linked {@link StoreEventSource} which is responsible for event
 * ordering.
 */
public class CacheEventDispatcherImpl<K, V> implements CacheEventDispatcher<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheEventDispatcherImpl.class);
  private final ExecutorService unOrderedExectuor;
  private final ExecutorService orderedExecutor;

  private final Map<EventType, List<EventListenerWrapper<K, V>>> syncListenersList = unmodifiableMap(allOf(EventType.class).stream()
    .collect(toMap(identity(), t -> new CopyOnWriteArrayList<>(), (a, b) -> { throw new AssertionError(); }, () -> new EnumMap<>(EventType.class))));
  private final Map<EventType, List<EventListenerWrapper<K, V>>> asyncListenersList = unmodifiableMap(allOf(EventType.class).stream()
    .collect(toMap(identity(), t -> new CopyOnWriteArrayList<>(), (a, b) -> { throw new AssertionError(); }, () -> new EnumMap<>(EventType.class))));

  private final StoreEventListener<K, V> eventListener = new StoreListener();

  private volatile Cache<K, V> listenerSource;
  private volatile StoreEventSource<K, V> storeEventSource;

  /**
   * Creates a new {@link CacheEventDispatcher} instance that will use the provided {@link ExecutorService} to handle
   * events firing.
   *
   * @param unOrderedExecutor the executor service used when ordering is not required
   * @param orderedExecutor the executor service used when ordering is required
   */
  public CacheEventDispatcherImpl(ExecutorService unOrderedExecutor, ExecutorService orderedExecutor) {
    this.unOrderedExectuor = unOrderedExecutor;
    this.orderedExecutor = orderedExecutor;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void registerCacheEventListener(CacheEventListener<? super K, ? super V> listener,
                                  EventOrdering ordering, EventFiring firing, EnumSet<EventType> forEventTypes) {
    EventListenerWrapper<K, V> wrapper = new EventListenerWrapper<>(listener, firing, ordering, forEventTypes);

    registerCacheEventListener(wrapper);
  }

  /**
   * Synchronized to make sure listener addition is atomic in order to prevent having the same listener registered
   * under multiple configurations
   *
   * @param wrapper the listener wrapper to register
   */
  private synchronized void registerCacheEventListener(EventListenerWrapper<K, V> wrapper) {

    if(allListeners().anyMatch(wrapper::equals)) {
      throw new IllegalStateException("Cache Event Listener already registered: " + wrapper.getListener());
    }

    boolean firstListener = !allListeners().findAny().isPresent();

    if (wrapper.isOrdered() && (firstListener || allListeners().noneMatch(EventListenerWrapper::isOrdered))) {
      storeEventSource.setEventOrdering(true);
    }

    switch (wrapper.getFiringMode()) {
      case ASYNCHRONOUS:
        wrapper.getEventTypes().forEach(type -> asyncListenersList.get(type).add(wrapper));
        break;
      case SYNCHRONOUS:
        if (!syncListeners().findAny().isPresent()) {
          storeEventSource.setSynchronous(true);
        }
        wrapper.getEventTypes().forEach(type -> syncListenersList.get(type).add(wrapper));
        break;
      default:
        throw new AssertionError("Unhandled EventFiring value: " + wrapper.getFiringMode());
    }

    if (firstListener) {
      storeEventSource.addEventListener(eventListener);
    }
  }

  private Stream<EventListenerWrapper<K, V>> allListeners() {
    return concat(asyncListeners(), syncListeners());
  }

  private Stream<EventListenerWrapper<K, V>> syncListeners() {
    return syncListenersList.values().stream().flatMap(Collection::stream);
  }

  private Stream<EventListenerWrapper<K, V>> asyncListeners() {
    return asyncListenersList.values().stream().flatMap(Collection::stream);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized  void deregisterCacheEventListener(CacheEventListener<? super K, ? super V> listener) {
    EventListenerWrapper<K, V> wrapper = new EventListenerWrapper<>(listener);

    boolean removed = Stream.of(asyncListenersList, syncListenersList)
      .flatMap(list -> list.values().stream())
      .map(list -> list.remove(wrapper))
      .reduce((a, b) -> a || b).orElse(false);

    if (!removed) {
      throw new IllegalStateException("Unknown cache event listener: " + listener);
    }

    if (!allListeners().findAny().isPresent()) {
      storeEventSource.setSynchronous(false);
      storeEventSource.setEventOrdering(false);
      storeEventSource.removeEventListener(eventListener);
    } else {
      if (allListeners().noneMatch(EventListenerWrapper::isOrdered)) {
        storeEventSource.setEventOrdering(false);
      }
      if (!syncListeners().findAny().isPresent()) {
        storeEventSource.setSynchronous(false);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void shutdown() {
    storeEventSource.removeEventListener(eventListener);
    storeEventSource.setEventOrdering(false);
    storeEventSource.setSynchronous(false);
    syncListenersList.values().forEach(Collection::clear);
    asyncListenersList.values().forEach(Collection::clear);
    unOrderedExectuor.shutdown();
    orderedExecutor.shutdown();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void setListenerSource(Cache<K, V> source) {
    this.listenerSource = source;
  }

  void onEvent(CacheEvent<K, V> event) {
    ExecutorService executor;
    if (storeEventSource.isEventOrdering()) {
      executor = orderedExecutor;
    } else {
      executor = unOrderedExectuor;
    }
    List<EventListenerWrapper<K, V>> asyncTargets = asyncListenersList.get(event.getType());
    if (!asyncTargets.isEmpty()) {
      executor.submit(new EventDispatchTask<>(event, asyncTargets));
    }
    List<EventListenerWrapper<K, V>> syncTargets = syncListenersList.get(event.getType());
    if (!syncTargets.isEmpty()) {
      Future<?> future = executor.submit(new EventDispatchTask<>(event, syncTargets));
      try {
        future.get();
      } catch (Exception e) {
        LOGGER.error("Exception received as result from synchronous listeners", e);
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  @SuppressWarnings("unchecked")
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> configurationChangeListenerList = new ArrayList<>();
    configurationChangeListenerList.add(event -> {
      if (event.getProperty().equals(CacheConfigurationProperty.ADD_LISTENER)) {
        registerCacheEventListener((EventListenerWrapper<K, V>)event.getNewValue());
      } else if (event.getProperty().equals(CacheConfigurationProperty.REMOVE_LISTENER)) {
        CacheEventListener<? super K, ? super V> oldListener = (CacheEventListener<? super K, ? super V>)event.getOldValue();
        deregisterCacheEventListener(oldListener);
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
        default:
          throw new AssertionError("Unexpected StoreEvent value: " + event.getType());
      }
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public synchronized void setStoreEventSource(StoreEventSource<K, V> eventSource) {
    this.storeEventSource = eventSource;
  }
}
