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
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.TimeSource;
import org.ehcache.spi.cache.Store;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.internal.verification.NoMoreInteractions;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Matchers.any;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author vfunshte
 *
 */
@SuppressWarnings({"unchecked", "rawtypes"})
public class CacheEventNotificationServiceImplTest {
  private CacheEventNotificationServiceImpl<Number, String> eventService;
  private CacheEventListener<Number, String> listener;
  private ExecutorService orderedExecutor;
  private ExecutorService unorderedExecutor;
  private Store<Number, String> store;

  @Before
  public void setUp() {
    orderedExecutor = Executors.newSingleThreadExecutor();
    unorderedExecutor = Executors.newCachedThreadPool();
    store = mock(Store.class);
    TimeSource timeSource = SystemTimeSource.INSTANCE;
    eventService = new CacheEventNotificationServiceImpl<Number, String>(store, new OrderedEventDispatcher<Number, String>(orderedExecutor),
        new UnorderedEventDispatcher<Number, String>(unorderedExecutor), timeSource);
    listener = mock(CacheEventListener.class);
  }

  @After
  public void tearDown() throws Exception {
    orderedExecutor.shutdownNow();
    unorderedExecutor.shutdownNow();
  }

  @Test
  public void testOrderedEventDelivery() {
    EnumSet<EventType> events = EnumSet.of(EventType.CREATED, EventType.UPDATED, EventType.REMOVED);
    eventService.registerCacheEventListener(listener, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, events);
    CacheEvent<Number, String> create = eventOfType(EventType.CREATED);
    CacheEvent<Number, String> update = eventOfType(EventType.UPDATED);
    CacheEvent<Number, String> remove = eventOfType(EventType.REMOVED);
    
    eventService.onEvent(remove);
    eventService.onEvent(create);
    eventService.onEvent(update);

    eventService.fireAllEvents();
    InOrder order = inOrder(listener);
    order.verify(listener).onEvent(remove);
    order.verify(listener).onEvent(create);
    order.verify(listener).onEvent(update);
  }

  @Test
  public void testUnorderedEventDelivery() {
    EnumSet<EventType> events = EnumSet.of(EventType.CREATED, EventType.UPDATED, EventType.REMOVED);
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, events);
    CacheEvent<Number, String> expectedEvents[] = 
        new CacheEvent[] { eventOfType(EventType.CREATED), eventOfType(EventType.UPDATED), eventOfType(EventType.REMOVED) };
    for (CacheEvent<Number, String> event: expectedEvents) {
      eventService.onEvent(event);
    }
    eventService.fireAllEvents();
    verify(listener).onEvent(expectedEvents[2]);
    verify(listener).onEvent(expectedEvents[1]);
    verify(listener).onEvent(expectedEvents[0]);
    verify(listener, new NoMoreInteractions()).onEvent(any(CacheEvent.class));
  }
  
  @Test
  public void testAsyncEventFiring() throws Exception {
    final CountDownLatch signal = new CountDownLatch(1);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        signal.await();
        return null;
      }
      
    }).when(listener).onEvent(any(CacheEvent.class));
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.ASYNCHRONOUS, EnumSet.of(EventType.CREATED));
    final CacheEvent<Number, String> create = eventOfType(EventType.CREATED);
    
    eventService.onEvent(create);

    // This is a bit redundant b/c if we got here, we are already detached from listener
    assertThat(signal.getCount(), is(1l));
    signal.countDown();
  }
  
  @Test
  public void testCheckEventType() {
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    CacheEvent<Number, String> create = eventOfType(EventType.CREATED);
    eventService.onEvent(create);
    verify(listener, Mockito.never()).onEvent(any(CacheEvent.class));
    
    CacheEvent<Number, String> evict = eventOfType(EventType.EVICTED);
    eventService.onEvent(evict);
    eventService.fireAllEvents();
    verify(listener).onEvent(evict);
  }
  
  @Test(expected=IllegalStateException.class)
  public void testDuplicateRegistration() {
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    eventService.registerCacheEventListener(listener, EventOrdering.ORDERED, EventFiring.ASYNCHRONOUS, EnumSet.of(EventType.EXPIRED));
  }
  
  @Test(expected=IllegalStateException.class)
  public void testUnknownListenerDeregistration() {
    eventService.deregisterCacheEventListener(listener);
  }
  
  @Test
  public void testDeregisterStopsNotification() {
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    CacheEvent<Number, String> evict = eventOfType(EventType.EVICTED);
    eventService.onEvent(evict);
    eventService.fireAllEvents();
    verify(listener).onEvent(evict);

    eventService.deregisterCacheEventListener(listener);
    eventService.onEvent(evict);
    eventService.fireAllEvents();
    verify(listener, new NoMoreInteractions()).onEvent(any(CacheEvent.class));
  }

  @Test
  public void testReleaseAllStopsNotification() {
    eventService.registerCacheEventListener(listener, 
        EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.CREATED));
    CacheEventListener<Number, String> otherLsnr = mock(CacheEventListener.class);
    eventService.registerCacheEventListener(otherLsnr, 
        EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.REMOVED));

    CacheEvent<Number, String> create = eventOfType(EventType.CREATED);
    eventService.onEvent(create);

    CacheEvent<Number, String> remove = eventOfType(EventType.REMOVED);
    eventService.onEvent(remove);

    eventService.fireAllEvents();
    verify(listener).onEvent(create);
    verify(otherLsnr).onEvent(remove);

    eventService.releaseAllListeners();

    eventService.onEvent(create);
    eventService.onEvent(remove);

    eventService.fireAllEvents();
    verify(listener, new NoMoreInteractions()).onEvent(any(CacheEvent.class));
    verify(otherLsnr, new NoMoreInteractions()).onEvent(any(CacheEvent.class));
  }

  @Test
  public void testRegisterEventListenerForStoreNotifications() {
    eventService.registerCacheEventListener(listener,
        EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EXPIRED));
    verify(store).enableStoreEventNotifications(any(StoreEventListener.class));
    eventService.deregisterCacheEventListener(listener);
    verify(store).disableStoreEventNotifications();
  }

  private static <K, V> CacheEvent<K, V> eventOfType(EventType type) {
    CacheEvent<K, V> event = mock(CacheEvent.class, type.name());
    when(event.getType()).thenReturn(type);
    when(event.getKey()).thenReturn((K)new Object());
    return event;
  }
}
