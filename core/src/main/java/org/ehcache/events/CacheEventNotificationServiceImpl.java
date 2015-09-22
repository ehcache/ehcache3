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
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.cache.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArraySet;
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
 * 
 * @author vfunshte
 */
public class CacheEventNotificationServiceImpl<K, V> implements CacheEventNotificationService<K, V> {

  private static final Logger LOGGER = LoggerFactory.getLogger(CacheEventNotificationServiceImpl.class);
  private static final int DEFAULT_EVENT_PROCESSING_QUEUE_COUNT = 4;
  private final StoreListener<K, V> storeListener = new StoreListener<K, V>();
  private final Store<K, V> store;
  private final TimeSource timeSource;
  private final int eventQueueCount;
  private final ConcurrentHashMap<Integer, ConcurrentLinkedQueue<CacheEventWrapper>> keyBasedEventQueueMap;
  private final EventDispatcher<K, V> unOrderedEventDispatcher;
  private final EventDispatcher<K, V> orderedEventDispatcher;
  private volatile EventThreadLocal eventThreadLocal;
  private final AtomicInteger orderedListenerCount = new AtomicInteger(0);
  private final Set<EventListenerWrapper> syncListenersSet = new CopyOnWriteArraySet<EventListenerWrapper>();
  private final Set<EventListenerWrapper> aSyncListenersSet = new CopyOnWriteArraySet<EventListenerWrapper>();

  public CacheEventNotificationServiceImpl(Store<K, V> store, OrderedEventDispatcher<K, V> orderedEventDispatcher, UnorderedEventDispatcher<K, V> unorderedEventDispatcher, TimeSource timeSource) {
    this(store, orderedEventDispatcher, unorderedEventDispatcher, DEFAULT_EVENT_PROCESSING_QUEUE_COUNT, timeSource);
  }

  public CacheEventNotificationServiceImpl(Store<K, V> store, OrderedEventDispatcher<K, V> orderedEventDispatcher,
                                           UnorderedEventDispatcher<K, V> unorderedEventDispatcher, int numberOfEventProcessingQueues, final TimeSource timeSource) {
    this.store = store;
    this.timeSource = timeSource;
    LOGGER.info("Setting Event Processing Queue Count to {}", numberOfEventProcessingQueues);
    eventThreadLocal = new NoOpEventThreadLocalImpl();
    this.eventQueueCount = numberOfEventProcessingQueues;
    storeListener.setEventNotificationService(this);
    this.unOrderedEventDispatcher = orderedEventDispatcher;
    this.orderedEventDispatcher = unorderedEventDispatcher;
    this.keyBasedEventQueueMap = new ConcurrentHashMap<Integer, ConcurrentLinkedQueue<CacheEventWrapper>>();
    for (int i = 0; i < this.eventQueueCount; i++) {
      this.keyBasedEventQueueMap.put(i , new ConcurrentLinkedQueue<CacheEventWrapper>());
    }
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
    EventListenerWrapper wrapper = new EventListenerWrapper(listener, firing, ordering, forEventTypes);
    if(wrapper.config.orderingMode() == EventOrdering.ORDERED) {
      orderedListenerCount.incrementAndGet();
    }

    if(aSyncListenersSet.contains(wrapper) || syncListenersSet.contains(wrapper)) {
      throw new IllegalStateException("Cache Event Listener already registered: " + listener);
    }

    if (firing == EventFiring.ASYNCHRONOUS) {
      aSyncListenersSet.add(wrapper);
    } else {
      syncListenersSet.add(wrapper);
    }
    if (doRegister) {
      store.enableStoreEventNotifications(storeListener);
    }
    synchronized (this) {
      if (eventThreadLocal instanceof NoOpEventThreadLocalImpl) {
        eventThreadLocal = new EventThreadLocalImpl();
      }
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
    EventListenerWrapper wrapper = new EventListenerWrapper(listener,
        EventFiring.ASYNCHRONOUS, EventOrdering.UNORDERED, EnumSet.allOf(EventType.class));
    boolean removed = false;
    for (EventListenerWrapper listenerWrapper : aSyncListenersSet) {
      if(listenerWrapper.equals(wrapper)) {
        aSyncListenersSet.remove(listenerWrapper);
        removed = true;
        if(listenerWrapper.config.orderingMode() == EventOrdering.ORDERED) {
          orderedListenerCount.decrementAndGet();
        }
        break;
      }
    }
    if (!removed) {
      for (EventListenerWrapper listenerWrapper : syncListenersSet) {
        if(listenerWrapper.equals(wrapper)) {
          syncListenersSet.remove(listenerWrapper);
          removed = true;
          if(listenerWrapper.config.orderingMode() == EventOrdering.ORDERED) {
            orderedListenerCount.decrementAndGet();
          }
          break;
        }
      }
    }
    synchronized (this) {
      if (!hasListeners()) {
        if (eventThreadLocal instanceof EventThreadLocalImpl) {
          eventThreadLocal = new NoOpEventThreadLocalImpl();
        }
      }
      store.disableStoreEventNotifications();
    }
    if(!removed) {
      throw new IllegalStateException("Unknown cache event listener: " + listener);
    }
  }

  // TODO this should be really the shutdown method for the service
  @Override
  public void releaseAllListeners() {
    orderedListenerCount.set(0);
    aSyncListenersSet.clear();
    syncListenersSet.clear();
    eventThreadLocal.cleanUp();
  }

  @Override
  public void setStoreListenerSource(Cache<K, V> source) {
    storeListener.setSource(source);
  }

  @Override
  public void onEvent(CacheEvent<K, V> event) {
    final EventType type = event.getType();
    LOGGER.trace("Cache Event notified for event type {}", type);
    CacheEventWrapper<K, V> cacheEventWrapper = new CacheEventWrapper<K, V>(event);
    if (hasListeners()) {
      eventThreadLocal.verifyOrderedDispatch(orderedListenerCount.get() != 0);
      eventThreadLocal.addToEventList(cacheEventWrapper);
      if (eventThreadLocal.isOrdered()) {
        ConcurrentLinkedQueue<CacheEventWrapper> eventWrapperQueue = getEventQueue(event.getKey());
        eventWrapperQueue.add(cacheEventWrapper);
      }
    }
  }

  private void dispatchEvent(CacheEventWrapper cacheEventWrapper) {
    cacheEventWrapper.markFireable();
    ConcurrentLinkedQueue<CacheEventWrapper> eventWrapperQueue = getEventQueue((K)cacheEventWrapper.cacheEvent.getKey());
    orderedEventWait(cacheEventWrapper, eventWrapperQueue);
    if (eventThreadLocal.isOrdered()) {
      while (!eventWrapperQueue.isEmpty() && eventWrapperQueue.peek().isFireable()) {
        CacheEventWrapper event = eventWrapperQueue.peek();
        actualDispatch(event, orderedEventDispatcher);
        if (event != eventThreadLocal.get().get(0)) {
          event.markFiredAndSignalCondition(true);
        } else {
          event.markFiredAndSignalCondition(false);
        }
        eventWrapperQueue.remove();
      }
    } else {
      actualDispatch(cacheEventWrapper, unOrderedEventDispatcher);
    }
  }

  private void orderedEventWait(CacheEventWrapper cacheEventWrapper, Queue<CacheEventWrapper> eventWrapperQueue) {
    if (eventThreadLocal.isOrdered()) {
      if (cacheEventWrapper != eventWrapperQueue.peek()) {
        cacheEventWrapper.waitTillFired();
      }
    }
  }

  private void actualDispatch(CacheEventWrapper event, EventDispatcher<K, V> eventDispatcher) {
    if (event.hasFailed()) {
      return;
    }
    Future<?> future = null;
    if (!syncListenersSet.isEmpty()) {
      future = eventDispatcher.dispatch(event, syncListenersSet);
    }
    if (!aSyncListenersSet.isEmpty()) {
      eventDispatcher.dispatch(event, aSyncListenersSet);
    }
    if (future != null) {
      try {
        future.get();
      } catch (Exception e) {
        LOGGER.error("Cache Event Listener failed for event type {} due to ", event.cacheEvent.getType(), e);
      }
    }
  }

  /**
   * @return true if at least one cache event listener is registered
   */
  @Override
  public boolean hasListeners() {
    return !syncListenersSet.isEmpty() || !aSyncListenersSet.isEmpty();
  }

  @Override
  public List<CacheConfigurationChangeListener> getConfigurationChangeListeners() {
    List<CacheConfigurationChangeListener> configurationChangeListenerList = new ArrayList<CacheConfigurationChangeListener>();
    configurationChangeListenerList.add(new CacheConfigurationChangeListener() {
      @Override
      public void cacheConfigurationChange(final CacheConfigurationChangeEvent event) {
        if (event.getProperty().equals(CacheConfigurationProperty.ADDLISTENER)) {
          EventListenerWrapper newListener = (EventListenerWrapper)event.getNewValue();
          CacheEventListenerConfiguration listenerConfiguration = newListener.config;
          registerCacheEventListener(newListener.getListener(), listenerConfiguration.orderingMode(),
              listenerConfiguration.firingMode(), listenerConfiguration.fireOn());
        } else if (event.getProperty().equals(CacheConfigurationProperty.REMOVELISTENER)) {
          CacheEventListener<K, V> oldListener = (CacheEventListener)event.getOldValue();
          deregisterCacheEventListener(oldListener);
        }
      }
    });
    return configurationChangeListenerList;
  }

  @Override
  public void fireAllEvents() {
    while (!eventThreadLocal.get().isEmpty()) {
      CacheEventWrapper cacheEventWrapper = eventThreadLocal.get().get(0);
      dispatchEvent(cacheEventWrapper);
      eventThreadLocal.get().remove(0);
    }
  }

  @Override
  public void processAndFireRemainingEvents() {
    for(CacheEventWrapper cacheEventWrapper : eventThreadLocal.get()) {
      if((cacheEventWrapper.cacheEvent.getType() == EventType.CREATED)
        || (cacheEventWrapper.cacheEvent.getType() == EventType.UPDATED)
        || (cacheEventWrapper.cacheEvent.getType() == EventType.REMOVED)) {
       cacheEventWrapper.markFailed();
      }
    }
    try {
      fireAllEvents();
    } finally {
      eventThreadLocal.cleanUp();
    }
  }

  private ConcurrentLinkedQueue<CacheEventWrapper> getEventQueue (K key) {
    int mapKeyForQueue = Math.abs(key.hashCode() % eventQueueCount);
    return keyBasedEventQueueMap.get(mapKeyForQueue);
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
}
