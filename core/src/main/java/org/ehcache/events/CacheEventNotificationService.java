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

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

/**
 * Per-cache component that manages cache event listener registrations, and provides event delivery based on desired
 * firing mode and ordering, for specified event types. 
 * <p>
 * <h5>Note on event ordering guarantees:</h5> {@link #onEvent(CacheEvent)} is assumed to be called from within a per-key
 * lock scope. If that is not the case, this facility has no means for maintaining event ordering consistent with source 
 * of such events. That is - listeners registered to receive events in the order they occurred in underlying store may be 
 * invoked in an order inconsistent with actual ordering of corresponding operations on said store.
 * <p>
 * Conversely, sending events to this service inside lock scope, when there are no registered listeners interested in 
 * ordered event delivery is harmless, i.e. event delivery to unordered listeners will still occur.
 * 
 * @author vfunshte
 */
public final class CacheEventNotificationService<K, V> {
  
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
  public void registerCacheEventListener(CacheEventListener<K, V> listener,
                                  EventOrdering ordering, EventFiring firing, EnumSet<EventType> forEventTypes) {
    if (!registeredListeners.add(new EventListenerWrapper<K, V>(listener, firing, ordering, forEventTypes))) {
      throw new IllegalStateException("Cache Event Listener already registered: " + listener);
    }
  }

  /**
   *  Allows for deregistering of a previously registered {@link org.ehcache.event.CacheEventListener} instance
   *
   * @param listener the listener to deregister
   *
   * @throws java.lang.IllegalStateException if the listener isn't already registered
   */
  public void deregisterCacheEventListener(CacheEventListener<K, V> listener) {
    if (!registeredListeners.remove(new EventListenerWrapper<K, V>(listener, 
        EventFiring.ASYNCHRONOUS, EventOrdering.UNORDERED, EnumSet.allOf(EventType.class)))) {
      throw new IllegalStateException("Unknown cache event listener: " + listener);
    }
  }

  // TODO this should be really the shutdown method for the service
  public void releaseAllListeners(CacheEventListenerFactory factory) {
    for (EventListenerWrapper<K, V> wrapper: registeredListeners) {
      factory.releaseEventListener(wrapper.listener);
      registeredListeners.remove(wrapper);
    }
  }

  public void onEvent(final CacheEvent<K, V> event) {
    final EventType type = event.getType();
    Map<EventListenerWrapper<K, V>, Future<?>> notificationResults = 
        new HashMap<CacheEventNotificationService.EventListenerWrapper<K,V>, Future<?>>(registeredListeners.size());
    
    for (final EventListenerWrapper<K, V> wrapper: registeredListeners) {
      if (!wrapper.config.fireOn().contains(type)) {
        continue;
      }
      Runnable notificationTask = new Runnable() {
        public void run() {
          wrapper.listener.onEvent(event);
        }
      };
      
      ExecutorService eventDelivery = wrapper.config.orderingMode().equals(EventOrdering.UNORDERED) ? unorderedDelivery : orderedDelivery;
      notificationResults.put(wrapper, eventDelivery.submit(notificationTask));
    }
    
    for (Map.Entry<EventListenerWrapper<K, V>, Future<?>> entry: notificationResults.entrySet()) {
      EventListenerWrapper<K, V> wrapper = entry.getKey();
      Future<?> f = entry.getValue();
      if ((EventFiring.SYNCHRONOUS.equals(wrapper.config.firingMode()))) {
        boolean interrupted = false;
        try {
          f.get();
        } catch (ExecutionException e) {
          // XXX probably not the right thing to do though - rethrowing listener exceptions isn't our business
          throw new RuntimeException(e);
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

  private final Set<EventListenerWrapper<K, V>> registeredListeners = new CopyOnWriteArraySet<EventListenerWrapper<K, V>>();
  private final ExecutorService orderedDelivery = Executors.newSingleThreadExecutor(DEFAULT_THREAD_FACTORY);
  private final ExecutorService unorderedDelivery = Executors.newCachedThreadPool(DEFAULT_THREAD_FACTORY);
  
  private static final ThreadFactory DEFAULT_THREAD_FACTORY = new ThreadFactory() {
    
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(true);
      return t;
    }
  };

  private static final class EventListenerWrapper<P, Q> {
    final CacheEventListener<P, Q> listener;
    final CacheEventListenerConfiguration config;
    
    EventListenerWrapper(CacheEventListener<P, Q> listener, final EventFiring firing, final EventOrdering ordering, 
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
    
    public int hashCode() {
      return listener.hashCode();
    }
    
    public boolean equals(Object other) {
      EventListenerWrapper<P, Q> l2 = (EventListenerWrapper<P, Q>)other;
      return listener.equals(l2.listener);
    }
  }

}
