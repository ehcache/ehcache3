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
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.spi.cache.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

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
 * 
 * @author vfunshte
 */
public class CacheEventNotificationServiceImpl<K, V> implements CacheEventNotificationService<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheEventNotificationServiceImpl.class);
  private final StoreListener<K, V> storeListener = new StoreListener<K, V>();
  private final Store<K, V> store;

  public CacheEventNotificationServiceImpl(ExecutorService orderedDelivery, ExecutorService unorderedDelivery, Store<K, V> store) {
    this.orderedDelivery = orderedDelivery;
    this.unorderedDelivery = unorderedDelivery;
    this.store = store;
    storeListener.setEventNotificationService(this);
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
    boolean doRegister = forEventTypes.contains(EventType.EVICTED) || forEventTypes.contains(EventType.EXPIRED);
    if (!registeredListeners.add(new EventListenerWrapper(listener, firing, ordering, forEventTypes))) {
      throw new IllegalStateException("Cache Event Listener already registered: " + listener);
    }
    if (doRegister) {
      store.enableStoreEventNotifications(storeListener);
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
    if (!registeredListeners.remove(new EventListenerWrapper(listener,
        EventFiring.ASYNCHRONOUS, EventOrdering.UNORDERED, EnumSet.allOf(EventType.class)))) {
      throw new IllegalStateException("Unknown cache event listener: " + listener);
    }
    if (!hasListeners()) {
      store.disableStoreEventNotifications();
    }
  }

  // TODO this should be really the shutdown method for the service
  @Override
  public void releaseAllListeners() {
    for (EventListenerWrapper wrapper: registeredListeners) {
      registeredListeners.remove(wrapper);
    }
  }

  @Override
  public void setStoreListenerSource(Cache<K, V> source) {
    storeListener.setSource(source);
  }

  @Override
  public void onEvent(final CacheEvent<K, V> event) {
    final EventType type = event.getType();
    LOGGER.trace("Cache Event notified for event type {}", type);
    Map<EventListenerWrapper, Future<?>> notificationResults = 
        new HashMap<CacheEventNotificationServiceImpl.EventListenerWrapper, Future<?>>(registeredListeners.size());
    
    for (final EventListenerWrapper wrapper: registeredListeners) {
      if (!wrapper.config.fireOn().contains(type)) {
        continue;
      }
      Runnable notificationTask = new Runnable() {
        @Override
        public void run() {
          CacheEventListener<K, V> listener = wrapper.getListener();
          listener.onEvent(event);
        }
      };
      
      ExecutorService eventDelivery = wrapper.config.orderingMode().equals(EventOrdering.UNORDERED) ? unorderedDelivery : orderedDelivery;
      notificationResults.put(wrapper, eventDelivery.submit(notificationTask));
    }
    
    for (Map.Entry<EventListenerWrapper, Future<?>> entry: notificationResults.entrySet()) {
      EventListenerWrapper wrapper = entry.getKey();
      Future<?> f = entry.getValue();
      if ((EventFiring.SYNCHRONOUS.equals(wrapper.config.firingMode()))) {
        boolean interrupted = false;
        try {
          f.get();
        } catch (ExecutionException e) {
            LOGGER.error("Cache Event Listener failed for event type {} due to ", type, e);
          // XXX delegate to resilience strategy (#52), and/or just log
        } catch (InterruptedException e) {
          interrupted = true;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }
  }

  /**
   * @return true if at least one cache event listener is registered
   */
  @Override
  public boolean hasListeners() {
    return !registeredListeners.isEmpty();
  }

  private final Set<EventListenerWrapper> registeredListeners = new CopyOnWriteArraySet<EventListenerWrapper>();
  private final ExecutorService orderedDelivery;
  private final ExecutorService unorderedDelivery;

  private static final class EventListenerWrapper {
    final CacheEventListener<?, ?> listener;
    final CacheEventListenerConfiguration config;
    
    EventListenerWrapper(CacheEventListener<?, ?> listener, final EventFiring firing, final EventOrdering ordering, 
        final EnumSet<EventType> forEvents) {
      this.listener = listener;
      this.config = new CacheEventListenerConfiguration() {
        
        @Override
        public Class<CacheEventListenerFactory> getServiceType() {
          return CacheEventListenerFactory.class;
        }
        
        @Override
        public EventOrdering orderingMode() {
          return ordering;
        }
        
        @Override
        public EventFiring firingMode() {
          return firing;
        }
        
        @Override
        public EnumSet<EventType> fireOn() {
          return forEvents;
        }
      };
    }
    
    @SuppressWarnings("unchecked")
    <K, V> CacheEventListener<K, V> getListener() {
      return (CacheEventListener<K, V>) listener;
    }

    @Override
    public int hashCode() {
      return listener.hashCode();
    }
    
    @Override
    public boolean equals(Object other) {
      if (!(other instanceof EventListenerWrapper)) {
        return false;
      }
      EventListenerWrapper l2 = (EventListenerWrapper)other;
      return listener.equals(l2.listener);
    }
  }

  private final class StoreListener<K, V> implements StoreEventListener<K, V> {

    private CacheEventNotificationService<K, V> eventNotificationService;
    private Cache<K, V> source;

    @Override
    public void onEviction(Cache.Entry<K, V> entry) {
      eventNotificationService.onEvent(CacheEvents.eviction(entry, this.source));
    }

    @Override
    public void onExpiration(Cache.Entry<K, V> entry) {
      eventNotificationService.onEvent(CacheEvents.expiry(entry, this.source));
    }

    public void setEventNotificationService(CacheEventNotificationService<K, V> eventNotificationService) {
      this.eventNotificationService = eventNotificationService;
    }

    public void setSource(Cache<K, V> source) {
      this.source = source;
    }
  }
}
