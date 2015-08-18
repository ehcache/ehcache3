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
import org.ehcache.event.CacheEventListenerProvider;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.cache.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

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
  private final TimeSource timeSource;
  private AtomicBoolean stopped = new AtomicBoolean(false);
  ConcurrentLinkedQueue<CacheEvent> eventQueue = new ConcurrentLinkedQueue<CacheEvent>();
  ConcurrentLinkedQueue<CacheEventWrapper> eventQueue1 = new ConcurrentLinkedQueue<CacheEventWrapper>();
  private final Thread processingThread;

  public CacheEventNotificationServiceImpl(ExecutorService orderedDelivery, ExecutorService unorderedDelivery, Store<K, V> store, TimeSource timeSource) {
    this.orderedDelivery = orderedDelivery;
    this.unorderedDelivery = unorderedDelivery;
    this.store = store;
    this.timeSource = timeSource;
    storeListener.setEventNotificationService(this);
    this.processingThread = new Thread(new QueueProcessingThread());
    this.processingThread.setDaemon(true);
    if (processingThread.isAlive()) {
      throw new RuntimeException("The thread with name " + processingThread.getName() + " already exists and is still running");
    }
    processingThread.start();
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
    addOrderedListenerToSet(listener, ordering);
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
        EventFiring.ASYNCHRONOUS, EventOrdering.UNORDERED, EnumSet.allOf(EventType.class))) && removeOrderedListenerFromSet(listener)) {
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
  public void stopEventService() {
    while (stopped.compareAndSet(false, true));
  }

  @Override
  public void onEvent(final CacheEvent<K, V> event) {
    final EventType type = event.getType();
    LOGGER.trace("Cache Event notified for event type {}", type);
    System.out.println("Adding to queue " + event.getType());
    CacheEventWrapper<K, V> cacheEventWrapper = new CacheEventWrapper<K, V>(event);
//    eventQueue.add(event);
    eventQueue1.add(cacheEventWrapper);
    EventThreadLocal.get().add(cacheEventWrapper);
  }

  /**
   * @return true if at least one cache event listener is registered
   */
  @Override
  public boolean hasListeners() {
    return !registeredListeners.isEmpty();
  }

  private final Set<EventListenerWrapper> registeredListeners = new CopyOnWriteArraySet<EventListenerWrapper>();
  private final HashSet<CacheEventListener> orderedListenersSet = new HashSet<CacheEventListener>();
  private final ExecutorService orderedDelivery;
  private final ExecutorService unorderedDelivery;
  
  private boolean addOrderedListenerToSet(CacheEventListener<? super K, ? super V> listener, EventOrdering ordering) {
    return ordering != EventOrdering.ORDERED || orderedListenersSet.add(listener);
  }
  
  private boolean removeOrderedListenerFromSet(CacheEventListener<? super K, ? super V> listener) {
    orderedListenersSet.remove(listener);
    return true;
  }
  
  private static final class EventListenerWrapper {
    final CacheEventListener<?, ?> listener;
    final CacheEventListenerConfiguration config;
    
    EventListenerWrapper(CacheEventListener<?, ?> listener, final EventFiring firing, final EventOrdering ordering, 
        final EnumSet<EventType> forEvents) {
      this.listener = listener;
      this.config = new CacheEventListenerConfiguration() {
        
        @Override
        public Class<CacheEventListenerProvider> getServiceType() {
          return CacheEventListenerProvider.class;
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
    public void onEviction(final K key, final Store.ValueHolder<V> valueHolder) {
      CacheEvent<K, V> cacheEvent = CacheEvents.eviction(key, valueHolder.value(), this.source);
      eventNotificationService.onEvent(cacheEvent);
    }

    @Override
    public void onExpiration(final K key, final Store.ValueHolder<V> valueHolder) {
      CacheEvent<K, V> cacheEvent = CacheEvents.expiry(key, valueHolder.value(), this.source);
      eventNotificationService.onEvent(cacheEvent);
    }

    public void setEventNotificationService(CacheEventNotificationService<K, V> eventNotificationService) {
      this.eventNotificationService = eventNotificationService;
    }

    public void setSource(Cache<K, V> source) {
      this.source = source;
    }
  }

  private final class QueueProcessingThread implements Runnable {
    @Override
    public void run() {
      while (!stopped.get()) {
        if(registeredListeners.isEmpty() && !eventQueue.isEmpty()) {
          System.out.println("Listeners are empty and no event is added yet!!!");
          CacheEvent cacheEvent = eventQueue.remove();
          markEventProcessedAndMoveOn();
        }
        if(!eventQueue.isEmpty()) {
          for (final EventListenerWrapper wrapper: registeredListeners) {
            System.out.println("Notifying listeners!!! " + wrapper.getListener() + " For event : " + eventQueue.peek().getKey());
            if (!wrapper.config.fireOn().contains(eventQueue.peek().getType())) {
              continue;
            }
            if(wrapper.config.firingMode() == EventFiring.SYNCHRONOUS) {
              waitForEventToGetFireableAndFire(eventQueue.peek(), wrapper.getListener());
            } else {
              try{
                wrapper.getListener().onEvent(markEventProcessedAndMoveOn());
              } catch (Exception e) {
                LOGGER.warn(wrapper.getListener() + " Failed to throw Event with Exception {}", e);
              } finally {
                markEventProcessedAndMoveOn();
              }
            }
          }
          System.out.println("Removing event : " + eventQueue.peek().getKey() + " " + eventQueue.peek().getType());
          if(!eventQueue.peek().isProcessed()) {
            eventQueue.peek().lockEvent();
            System.out.println("Marking it processed " + eventQueue.peek().getType());
            eventQueue.peek().markProcessed();
            eventQueue.peek().signalProcessedCondition();
            eventQueue.peek().unlockEvent();
          }
          eventQueue.remove();
        }
      }
    }
  }

  private boolean waitForEventToGetFireableAndFire(CacheEvent<?, ?> event, CacheEventListener<?, ?> listener) {
    event.lockEvent();
    try {
      while (!event.isFireable() && !event.hasFailed()) {
        System.out.println("Initiating wait for "+ event.getType() + " blah  " + event.getKey());
        event.awaitFireableCondition();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    } finally {
      fireEventOnListener(listener);
      event.unlockEvent();
      markEventProcessedAndMoveOn();
    }
    return event.isFireable();
  }
  
  private CacheEvent markEventProcessedAndMoveOn() {
    CacheEvent event = eventQueue.peek();
    if (!event.isProcessed()) {
      event.lockEvent();
      event.markProcessed();
      event.signalProcessedCondition();
      event.unlockEvent();
    }
    return event;
  }
  
  private void fireEventOnListener(CacheEventListener<?, ?> listener) {
    try{
      listener.onEvent(markEventProcessedAndMoveOn());
    } catch (Exception e) {
      LOGGER.warn(listener + " Failed to throw Event with Exception {}", e);
    } finally {
      markEventProcessedAndMoveOn();
    }
  }
}
