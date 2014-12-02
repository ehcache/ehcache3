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

package org.ehcache;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.times;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.argThat;

import static org.hamcrest.Matchers.is;

import static org.junit.Assert.assertThat;

import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerFactory;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheWriterException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.util.StatisticsThreadPoolUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.internal.verification.NoMoreInteractions;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

@SuppressWarnings("unchecked")
public class EhcacheEventTest {
  
  private Ehcache<Number, String> cache;
  private Store<Number, String> store;
  private CacheEventNotificationService<Number, String> eventNotifier;

  @Before
  public void setUp() throws Exception {
    store = mock(Store.class);
    eventNotifier = mock(CacheEventNotificationService.class);
    CacheWriter<Number, String> writer = mock(CacheWriter.class);

    cache = new Ehcache<Number, String>(
        newCacheConfigurationBuilder().buildConfig(Number.class, String.class), store, null, writer, eventNotifier,
        StatisticsThreadPoolUtil.getDefaultStatisticsExecutorService());
    cache.init();
  }
  
  @After
  public void tearDown() {
    // Make sure no more events have been sent
    verify(eventNotifier, new NoMoreInteractions()).onEvent(any(CacheEvent.class));
  }

  @Test
  public void testRuntimeConfigDelegatesToNotifier() {
    CacheEventListener<Number, String> listener = mock(CacheEventListener.class);
    cache.getRuntimeConfiguration().registerCacheEventListener(listener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.CREATED));
    verify(eventNotifier).registerCacheEventListener(eq(listener), eq(EventOrdering.UNORDERED), eq(EventFiring.SYNCHRONOUS),
        eq(EnumSet.of(EventType.CREATED)));
    
    cache.getRuntimeConfiguration().deregisterCacheEventListener(listener);
    verify(eventNotifier).deregisterCacheEventListener(listener);
    verify(eventNotifier).hasListeners();
    
    cache.getRuntimeConfiguration().releaseAllEventListeners(mock(CacheEventListenerFactory.class));
    verify(eventNotifier).releaseAllListeners(any(CacheEventListenerFactory.class));
  }
  
  @Test
  public void testLazyStoreEventListening() {
    verify(store, never()).enableStoreEventNotifications(any(StoreEventListener.class));
    CacheEventListener<Number, String> expiryListener = mock(CacheEventListener.class);
    
    when(eventNotifier.hasListeners()).thenReturn(false);
    cache.getRuntimeConfiguration().registerCacheEventListener(expiryListener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, 
        EnumSet.of(EventType.EXPIRED));
    
    CacheEventListener<Number, String> evictionListener = mock(CacheEventListener.class);
    cache.getRuntimeConfiguration().registerCacheEventListener(evictionListener, EventOrdering.UNORDERED, EventFiring.SYNCHRONOUS, 
        EnumSet.of(EventType.EVICTED));
    verify(eventNotifier).registerCacheEventListener(eq(expiryListener), 
        eq(EventOrdering.UNORDERED), eq(EventFiring.SYNCHRONOUS),
        eq(EnumSet.of(EventType.EXPIRED)));
    verify(eventNotifier).registerCacheEventListener(eq(evictionListener), 
        eq(EventOrdering.UNORDERED), eq(EventFiring.SYNCHRONOUS),
        eq(EnumSet.of(EventType.EVICTED)));
    
    verify(store, times(2)).enableStoreEventNotifications(any(StoreEventListener.class));
    
    when(eventNotifier.hasListeners()).thenReturn(true);
    cache.getRuntimeConfiguration().deregisterCacheEventListener(evictionListener);
    verify(store, never()).disableStoreEventNotifications();
    
    when(eventNotifier.hasListeners()).thenReturn(false);
    cache.getRuntimeConfiguration().deregisterCacheEventListener(expiryListener);
    verify(store).disableStoreEventNotifications();
    verify(eventNotifier, times(2)).hasListeners();
    verify(eventNotifier, times(2)).deregisterCacheEventListener(any(CacheEventListener.class));
  }

  @Test
  public void testPutNoPreviousEntry() throws Exception {
    Number key = 1;
    String value = "one";
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    cache.put(key, value);
    verify(eventNotifier).onEvent(eventMatching(EventType.CREATED, key, value, null));
  }

  @Test
  public void testPutOverExistingEntry() throws Exception {
    Number key = 1;
    String value = "one";
    final String oldValue = "zero";
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], oldValue);
        return null;
      }
    });
    cache.put(key, value);
    verify(eventNotifier).onEvent(eventMatching(EventType.UPDATED, key, value, oldValue));
  }
  
  @Test
  @Ignore // XXX - revisit later
  public void testPutThrowsOnCompute() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenThrow(new CacheAccessException("boom"));
    cache.put(1, "one");
    verify(eventNotifier, never()).onEvent(any(CacheEvent.class));
  }
  
  @Test(expected=CacheWriterException.class)
  public void testPutThrowsOnWrite() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    doThrow(new Exception()).when(cache.getCacheWriter()).write(any(Number.class), anyString());
    cache.put(1, "one");
  }

  @Test
  public void testRemove() throws Exception {
    Number key = 1;
    final String value = "one";

    cache.remove(key);
    verify(eventNotifier, never()).onEvent(any(CacheEvent.class));
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], value);
        return null;
      }
    });
    cache.remove(key);
    verify(eventNotifier).onEvent(eventMatching(EventType.REMOVED, key, value, value));
  }
  
  @Test(expected=CacheWriterException.class)
  public void testRemoveThrowsOnWrite() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    doThrow(new Exception()).when(cache.getCacheWriter()).delete(any(Number.class));
    cache.remove(1);
  }

  @Test
  public void testReplace() throws Exception {
    final String oldValue = "foo";
    final String newValue = "bar";
    final Number key = 1;

    assertThat(cache.replace(key, newValue), is((String)null));
    verify(eventNotifier, never()).onEvent(any(CacheEvent.class));

    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, CharSequence, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], oldValue);
        return null;
      }
    });

    assertThat(cache.replace(key, newValue), is(oldValue));
    verify(eventNotifier).onEvent(eventMatching(EventType.UPDATED, key, newValue, oldValue));
  }
  
  @Test(expected=CacheWriterException.class)
  public void testReplaceThrowsOnWrite() throws Exception {
    final String expected = "old";
    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], expected);
        return null;
      }
    });
    doThrow(new Exception()).when(cache.getCacheWriter()).write(any(Number.class), anyString());
    cache.replace(1, "bar");
  }

  @Test
  public void testThreeArgReplaceMatch() throws Exception {
    final String cachedValue = "cached";
    final String newValue = "toReplace";

    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        final String applied = function.apply((Number)invocation.getArguments()[0], cachedValue);
        final Store.ValueHolder mock = mock(Store.ValueHolder.class);
        when(mock.value()).thenReturn(applied);
        return mock;
      }
    });
    Number key = 1;
    assertThat(cache.replace(key, cachedValue, newValue), is(true));
    verify(eventNotifier).onEvent(eventMatching(EventType.UPDATED, key, newValue, cachedValue));
  }
  
  @Test
  public void testThreeArgReplaceKeyNotInCache() throws Exception {
    final String oldValue = "cached";
    final String newValue = "toReplace";

    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });

    Number key = 1;
    assertThat(cache.replace(key, oldValue, newValue), is(false));
    verify(eventNotifier, never()).onEvent(eventMatching(EventType.UPDATED, key, newValue, oldValue));
  }
  
  @Test
  public void testThreeArgReplaceWriteUnsuccessful() throws Exception {
    final String oldValue = "cached";
    final String newValue = "toReplace";
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });

    assertThat(cache.replace(1, oldValue, newValue), is(false));
    verify(eventNotifier, never()).onEvent(any(CacheEvent.class));
  }
  
  @Test(expected=CacheWriterException.class)
  public void testThreeArgReplaceThrowsOnWrite() throws Exception {
    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        return function.apply((Number)invocation.getArguments()[0], "old");
      }
    });
    doThrow(new Exception()).when(cache.getCacheWriter()).write(any(Number.class), anyString());
    cache.replace(1, "old", "new");
  }

  @Test
  public void testPutIfAbsent() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        final String applied = function.apply((Number)invocation.getArguments()[0]);
        final Store.ValueHolder mock = mock(Store.ValueHolder.class);
        when(mock.value()).thenReturn(applied);
        return mock;
      }
    });
    Number key = 1;
    assertThat(cache.putIfAbsent(key, "foo"), nullValue());
    verify(eventNotifier).onEvent(eventMatching(EventType.CREATED, key, "foo", null));
  }
  
  @Test(expected=CacheWriterException.class)
  public void testPutIfAbsentThrowsOnWrite() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        function.apply((Number)invocation.getArguments()[0]);
        return null;
      }
    });
    doThrow(new Exception()).when(cache.getCacheWriter()).write(any(Number.class), anyString());
    cache.putIfAbsent(1, "one");
  }
  
  @Test
  public void testTwoArgRemoveMatch() throws Exception {
    final String cachedValue = "cached";
    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        final String applied = function.apply((Number)invocation.getArguments()[0], cachedValue);
        final Store.ValueHolder mock = mock(Store.ValueHolder.class);
        when(mock.value()).thenReturn(applied);
        return mock;
      }
    });
    Number key = 1;
    assertThat(cache.remove(key, cachedValue), is(true));
    verify(eventNotifier).onEvent(eventMatching(EventType.REMOVED, key, cachedValue, cachedValue));
  }
  
  @Test
  public void testTwoArgRemoveKeyNotInCache() throws Exception {
    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    String toRemove = "foo";
    Number key = 1;
    assertThat(cache.remove(key, toRemove), is(false));
    verify(eventNotifier, never()).onEvent(eventMatching(EventType.REMOVED, key, toRemove, toRemove));
  }
  
  @Test
  public void testTwoArgRemoveWriteUnsuccessful() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    String toRemove = "foo";
    assertThat(cache.remove(1, toRemove), is(false));
    verify(cache.getCacheWriter(), never()).delete(any(Number.class));
    verify(eventNotifier, never()).onEvent(any(CacheEvent.class));
  }
  
  @Test(expected=CacheWriterException.class)
  public void testTwoArgRemoveThrowsOnWrite() throws Exception {
    final String expected = "foo";
    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        final String applied = function.apply((Number)invocation.getArguments()[0], expected);
        final Store.ValueHolder mock = mock(Store.ValueHolder.class);
        when(mock.value()).thenReturn(applied);
        return mock;
      }
    });
    when(cache.getCacheWriter().delete(any(Number.class))).thenThrow(new Exception());
    cache.remove(1, expected);
  }

  @Test
  public void testOrderedEventFiring() throws Exception {
    final Number key = 1;
    int numEvents = 10;
    final AtomicReference<String> curValue = new AtomicReference<String>();
    Collection<Callable<Void>> tasks = new ArrayList<Callable<Void>>(numEvents);
    final String values[] = new String[numEvents];
    for (int i = 0; i < numEvents; i++) {
      values[i] = "value-" + i;
      final int idx = i;
      tasks.add(new Callable<Void>() {
        
        @Override
        public Void call() {
          cache.put(key, values[idx]);
          return null;
        }
      });
    }
    
    final List<String> expectedOrder = new ArrayList<String>(numEvents);
    
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        synchronized (key) {
          String oldValue = curValue.get();
          expectedOrder.add(oldValue);
          String newValue = function.apply((Number)invocation.getArguments()[0], oldValue);
          curValue.set(newValue);
        }
        return null;
      }
    });
    
    ExecutorService tPool = Executors.newCachedThreadPool();
    tPool.invokeAll(tasks);
    
    InOrder order = inOrder(eventNotifier);
    for (final String oldValue: expectedOrder) {
      order.verify(eventNotifier).onEvent(argThat(new ArgumentMatcher<CacheEvent<Number,String>>() {

        @Override
        public boolean matches(Object argument) {
          CacheEvent<Number, String> event = (CacheEvent<Number, String>)argument;
          Cache.Entry<Number, String> entry = event.getEntry();
          return entry.getKey().equals(key) && (oldValue == null ? event.getType() == EventType.CREATED 
              && event.getPreviousValue() == null : event.getType() == EventType.UPDATED &&
            event.getPreviousValue().equals(oldValue));
        }
        
      }));
    }
    tPool.shutdown();
  }

  private static BiFunction<Number, String, String> anyBiFunction() {
    return any(BiFunction.class);
  }

  private Function<Number, String> anyFunction() {
    return any(Function.class);
  }

  private static <A, B, T> BiFunction<A, B, T> asBiFunction(InvocationOnMock in) {
    return (BiFunction<A, B, T>)in.getArguments()[1];
  }
  
  private static <A, T> Function<A, T> asFunction(InvocationOnMock in) {
    return (Function<A, T>)in.getArguments()[1];
  }

  private static <K, V> CacheEvent<K, V> eventMatching(final EventType type, final K key, final V value, final V oldValue) {
    return argThat(new ArgumentMatcher<CacheEvent<K, V>>() {

      @Override
      public boolean matches(Object argument) {
        CacheEvent<K, V> event = (CacheEvent<K, V>)argument;
        Cache.Entry<K, V> entry = event.getEntry();
        return type == event.getType() && entry.getKey().equals(key) && entry.getValue().equals(value) && (event.getPreviousValue() == null ? oldValue == null : 
          event.getPreviousValue().equals(oldValue));
      }
      
    });
  }
}
