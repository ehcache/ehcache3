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

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.core.spi.store.events.StoreEventSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


@SuppressWarnings({"unchecked", "rawtypes"})
public class CacheEventDispatcherImplTest {
  private CacheEventDispatcherImpl<Number, String> eventService;
  private CacheEventListener<Number, String> listener;
  private ExecutorService orderedExecutor;
  private ExecutorService unorderedExecutor;
  private StoreEventSource storeEventDispatcher;

  @Before
  public void setUp() {
    orderedExecutor = mock(ExecutorService.class);
    unorderedExecutor = mock(ExecutorService.class);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        EventDispatchTask task = (EventDispatchTask) invocation.getArguments()[0];
        task.run();
        return null;
      }
    }).when(unorderedExecutor).submit(any(Runnable.class));
    storeEventDispatcher = mock(StoreEventSource.class);
    eventService = new CacheEventDispatcherImpl<Number, String>(unorderedExecutor, orderedExecutor);
    eventService.setStoreEventSource(storeEventDispatcher);
    listener = mock(CacheEventListener.class);
  }

  @After
  public void tearDown() throws Exception {
    orderedExecutor.shutdownNow();
    unorderedExecutor.shutdownNow();
  }

  @Test
  public void testAsyncEventFiring() throws Exception {
    eventService = new CacheEventDispatcherImpl<Number, String>(Executors.newCachedThreadPool(), orderedExecutor);
    eventService.setStoreEventSource(storeEventDispatcher);
    final CountDownLatch signal = new CountDownLatch(1);
    final CountDownLatch signal2 = new CountDownLatch(1);
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        if (!signal.await(2, TimeUnit.SECONDS)) {
          return null;
        } else {
          signal2.countDown();
          return null;
        }
      }

    }).when(listener).onEvent(any(CacheEvent.class));
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.ASYNCHRONOUS, EnumSet.of(EventType.CREATED));
    final CacheEvent<Number, String> create = eventOfType(EventType.CREATED);

    eventService.onEvent(create);

    signal.countDown();
    if (!signal2.await(2, TimeUnit.SECONDS)) {
      fail("event handler never triggered latch - are we synchronous?");
    }
  }

  @Test
  public void testCheckEventType() {
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    CacheEvent<Number, String> create = eventOfType(EventType.CREATED);
    eventService.onEvent(create);
    verify(listener, Mockito.never()).onEvent(any(CacheEvent.class));

    CacheEvent<Number, String> evict = eventOfType(EventType.EVICTED);
    eventService.onEvent(evict);
    verify(listener).onEvent(evict);
  }

  @Test
  public void testListenerRegistrationEnablesStoreEvents() {
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.ASYNCHRONOUS, EnumSet.allOf(EventType.class));
    verify(storeEventDispatcher).addEventListener(any(StoreEventListener.class));
  }

  @Test
  public void testOrderedListenerRegistrationTogglesOrderedOnStoreEvents() {
    eventService.registerCacheEventListener(listener, EventOrdering.ORDERED, EventFiring.ASYNCHRONOUS, EnumSet.allOf(EventType.class));
    verify(storeEventDispatcher).setEventOrdering(true);
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
  public void testDeregisterLastListenerStopsStoreEvents() {
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    eventService.deregisterCacheEventListener(listener);
    verify(storeEventDispatcher).removeEventListener(any(StoreEventListener.class));
  }

  @Test
  public void testDeregisterLastOrderedListenerTogglesOffOrderedStoreEvents() {
    eventService.registerCacheEventListener(listener, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    eventService.deregisterCacheEventListener(listener);
    verify(storeEventDispatcher).setEventOrdering(false);
  }

  @Test
  public void testDeregisterNotLastOrderedListenerDoesNotToggleOffOrderedStoreEvents() {
    eventService.registerCacheEventListener(listener, EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    eventService.registerCacheEventListener(mock(CacheEventListener.class), EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    eventService.deregisterCacheEventListener(listener);
    verify(storeEventDispatcher, never()).setEventOrdering(false);
  }

  @Test
  public void testDeregisterNotLastListenerDoesNotStopStoreEvents() {
    eventService.registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    eventService.registerCacheEventListener(mock(CacheEventListener.class), EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EVICTED));
    eventService.deregisterCacheEventListener(listener);
    verify(storeEventDispatcher, never()).removeEventListener(any(StoreEventListener.class));
  }

  @Test
  public void testShutdownDisableStoreEventsAndShutsDownOrderedExecutor() {
    eventService.registerCacheEventListener(listener,
        EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.CREATED));
    CacheEventListener<Number, String> otherLsnr = mock(CacheEventListener.class);
    eventService.registerCacheEventListener(otherLsnr,
        EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.REMOVED));

    eventService.shutdown();

    InOrder inOrder = inOrder(storeEventDispatcher, orderedExecutor);
    inOrder.verify(storeEventDispatcher).removeEventListener(any(StoreEventListener.class));
    inOrder.verify(storeEventDispatcher).setEventOrdering(false);
    inOrder.verify(orderedExecutor).shutdown();
    inOrder.verifyNoMoreInteractions();
  }

  private static <K, V> CacheEvent<K, V> eventOfType(EventType type) {
    CacheEvent<K, V> event = mock(CacheEvent.class, type.name());
    when(event.getType()).thenReturn(type);
    when(event.getKey()).thenReturn((K)new Object());
    return event;
  }
}
