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

package org.ehcache.impl.store;

import org.ehcache.event.EventType;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventFilter;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.hamcrest.Matcher;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.ehcache.impl.internal.util.Matchers.eventOfType;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * DefaultStoreEventDispatcherTest
 */
public class DefaultStoreEventDispatcherTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStoreEventDispatcherTest.class);

  @Test
  public void testRegistersOrderingChange() {
    DefaultStoreEventDispatcher<Object, Object> dispatcher = new DefaultStoreEventDispatcher<>(1);

    assertThat(dispatcher.isEventOrdering(), is(false));
    dispatcher.setEventOrdering(true);
    assertThat(dispatcher.isEventOrdering(), is(true));
    dispatcher.setEventOrdering(false);
    assertThat(dispatcher.isEventOrdering(), is(false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListenerNotifiedUnordered() {
    DefaultStoreEventDispatcher<String, String> dispatcher = new DefaultStoreEventDispatcher<>(1);
    @SuppressWarnings("unchecked")
    StoreEventListener<String, String> listener = mock(StoreEventListener.class);
    dispatcher.addEventListener(listener);

    StoreEventSink<String, String> sink = dispatcher.eventSink();
    sink.created("test", "test");
    dispatcher.releaseEventSink(sink);

    verify(listener).onEvent(any(StoreEvent.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testListenerNotifiedOrdered() {
    DefaultStoreEventDispatcher<String, String> dispatcher = new DefaultStoreEventDispatcher<>(1);
    @SuppressWarnings("unchecked")
    StoreEventListener<String, String> listener = mock(StoreEventListener.class);
    dispatcher.addEventListener(listener);
    dispatcher.setEventOrdering(true);

    StoreEventSink<String, String> sink = dispatcher.eventSink();
    sink.created("test", "test");
    dispatcher.releaseEventSink(sink);

    verify(listener).onEvent(any(StoreEvent.class));
  }

  @Test
  public void testEventFiltering() {
    DefaultStoreEventDispatcher<String, String> dispatcher = new DefaultStoreEventDispatcher<>(1);
    @SuppressWarnings("unchecked")
    StoreEventListener<String, String> listener = mock(StoreEventListener.class, withSettings().verboseLogging());
    dispatcher.addEventListener(listener);

    @SuppressWarnings("unchecked")
    StoreEventFilter<String, String> filter = mock(StoreEventFilter.class);
    when(filter.acceptEvent(eq(EventType.CREATED), anyString(), ArgumentMatchers.<String>isNull(), anyString())).thenReturn(true);
    when(filter.acceptEvent(eq(EventType.REMOVED), anyString(), anyString(), anyString())).thenReturn(false);
    dispatcher.addEventFilter(filter);

    StoreEventSink<String, String> sink = dispatcher.eventSink();
    sink.removed("gone", () -> "really gone");
    sink.created("new", "and shiny");
    dispatcher.releaseEventSink(sink);

    Matcher<StoreEvent<String, String>> matcher = eventOfType(EventType.CREATED);
    verify(listener).onEvent(argThat(matcher));
    verifyNoMoreInteractions(listener);
  }

  @Test
  public void testOrderedEventDelivery() throws Exception {
    final DefaultStoreEventDispatcher<Long, Boolean> dispatcher = new DefaultStoreEventDispatcher<>(4);
    dispatcher.setEventOrdering(true);
    final ConcurrentHashMap<Long, Long> map = new ConcurrentHashMap<>();
    final long[] keys = new long[] { 1L, 42L, 256L };
    map.put(keys[0], 125L);
    map.put(keys[1], 42 * 125L);
    map.put(keys[2], 256 * 125L);

    final ConcurrentHashMap<Long, Long> resultMap = new ConcurrentHashMap<>(map);
    dispatcher.addEventListener(event -> {
      if (event.getNewValue()) {
        resultMap.compute(event.getKey(), (key, value) -> value + 10L);
      } else {
        resultMap.compute(event.getKey(), (key, value) -> 7L - value);
      }
    });

    final long seed = new Random().nextLong();
    LOGGER.info("Starting test with seed {}", seed);

    int workers = Runtime.getRuntime().availableProcessors() + 2;
    final CountDownLatch latch = new CountDownLatch(workers);
    for (int i = 0; i < workers; i++) {
      final int index =i;
      new Thread(() -> {
        Random random = new Random(seed * index);
        for (int j = 0; j < 10000; j++) {
          int keyIndex = random.nextInt(3);
          final StoreEventSink<Long, Boolean> sink = dispatcher.eventSink();
          if (random.nextBoolean()) {
            map.compute(keys[keyIndex], (key, value) -> {
              long newValue = value + 10L;
              sink.created(key, true);
              return newValue;
            });
          } else {
            map.compute(keys[keyIndex], (key, value) -> {
              long newValue = 7L - value;
              sink.created(key, false);
              return newValue;
            });
          }
          dispatcher.releaseEventSink(sink);
        }
        latch.countDown();
      }).start();
    }

    latch.await();

    LOGGER.info("\n\tResult map {} \n\tWork map {}", resultMap, map);

    assertThat(resultMap, is(map));
  }
}
