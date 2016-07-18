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

package org.ehcache.core;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.config.BaseCacheConfiguration;
import org.ehcache.core.config.ResourcePoolsHelper;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;


/**
 * @author vfunshteyn
 */
public class EhcacheLoaderWriterTest {
  private EhcacheWithLoaderWriter<Number, String> cache;
  private Store<Number, String> store;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    store = mock(Store.class);
    CacheLoaderWriter<Number, String> loaderWriter = mock(CacheLoaderWriter.class);
    final CacheConfiguration<Number, String> config = new BaseCacheConfiguration<Number, String>(Number.class, String.class, null,
        null, null, ResourcePoolsHelper.createHeapOnlyPools());
    CacheEventDispatcher<Number, String> notifier = mock(CacheEventDispatcher.class);
    cache = new EhcacheWithLoaderWriter<Number, String>(
        config, store, loaderWriter, notifier, LoggerFactory.getLogger(EhcacheWithLoaderWriter.class + "-" + "EhcacheLoaderWriterTest"));
    cache.init();
  }

  @Test
  public void testGet() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        function.apply((Number)invocation.getArguments()[0]);
        return null;
      }
    });
    cache.get(1);
    verify(cache.getCacheLoaderWriter()).load(1);
  }

  @Test
  public void testGetThrowsOnCompute() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenThrow(new StoreAccessException("boom"));
    String expected = "foo";
    when((String)cache.getCacheLoaderWriter().load(any(Number.class))).thenReturn(expected);
    assertThat(cache.get(1), is(expected));
    verify(store).remove(1);
  }

  @Test(expected=CacheLoadingException.class)
  public void testGetThrowsOnLoad() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        try {
          function.apply((Number)invocation.getArguments()[0]);
        } catch (StorePassThroughException e) {
          throw e.getCause();
        }
        return null;
      }
    });
    when(cache.getCacheLoaderWriter().load(any(Number.class))).thenThrow(new Exception());
    cache.get(1);
  }

  @Test
  public void testPut() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    cache.put(1, "one");
    verify(cache.getCacheLoaderWriter()).write(1, "one");
  }

  @Test
  public void testPutThrowsOnCompute() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenThrow(new StoreAccessException("boom"));
    cache.put(1, "one");
    verify(store).remove(1);
    verify(cache.getCacheLoaderWriter()).write(1, "one");
  }

  @Test(expected=CacheWritingException.class)
  public void testPutThrowsOnWrite() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        try {
          function.apply((Number)invocation.getArguments()[0], null);
        } catch (StorePassThroughException e) {
          throw e.getCause();
        }
        return null;
      }
    });
    doThrow(new Exception()).when(cache.getCacheLoaderWriter()).write(any(Number.class), anyString());
    cache.put(1, "one");
  }

  @Test
  public void testRemove() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    cache.remove(1);
    verify(cache.getCacheLoaderWriter()).delete(1);
  }

  @Test
  public void testRemoveThrowsOnCompute() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenThrow(new StoreAccessException("boom"));
    cache.remove(1);
    verify(store).remove(1);
    verify(cache.getCacheLoaderWriter()).delete(1);
  }

  @Test(expected=CacheWritingException.class)
  public void testRemoveThrowsOnWrite() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        try {
          function.apply((Number)invocation.getArguments()[0], null);
        } catch (StorePassThroughException e) {
          throw e.getCause();
        }
        return null;
      }
    });
    doThrow(new Exception()).when(cache.getCacheLoaderWriter()).delete(any(Number.class));
    cache.remove(1);
  }

  @Test
  public void testPutIfAbsent_present() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        Number key = (Number) invocation.getArguments()[0];
        if (!key.equals(1)) {
          function.apply(key);
        }
        return null;
      }
    });

    cache.putIfAbsent(1, "foo");
    verifyZeroInteractions(cache.getCacheLoaderWriter());
  }

  @Test
  public void testPutIfAbsent_absent() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        Number key = (Number) invocation.getArguments()[0];
        function.apply(key);
        return null;
      }
    });

    cache.putIfAbsent(1, "foo");
    verify(cache.getCacheLoaderWriter()).load(1);
    verify(cache.getCacheLoaderWriter()).write(1, "foo");
  }

  @Test
  public void testPutIfAbsentThrowsOnCompute() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenThrow(new StoreAccessException("boom"));
    cache.putIfAbsent(1, "one");
    verify(cache.getCacheLoaderWriter()).write(1, "one");
    verify(store).remove(1);
  }

  @Test(expected=CacheWritingException.class)
  public void testPutIfAbsentThrowsOnWrite() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        try {
          function.apply((Number)invocation.getArguments()[0]);
        } catch (StorePassThroughException e) {
          throw e.getCause();
        }
        return null;
      }
    });
    doThrow(new Exception()).when(cache.getCacheLoaderWriter()).write(any(Number.class), anyString());
    cache.putIfAbsent(1, "one");
  }

  @Test
  public void testTwoArgRemoveMatch() throws Exception {
    final String cachedValue = "cached";
    when(store.compute(any(Number.class), anyBiFunction(), anyNullaryFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], cachedValue);
        return null;
      }
    });
    assertThat(cache.remove(1, cachedValue), is(true));
    verify(cache.getCacheLoaderWriter()).delete(1);
  }

  @Test
  public void testTwoArgRemoveKeyNotInCache() throws Exception {
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
    verify(cache.getCacheLoaderWriter(), never()).delete(1);
  }

  @Test
  public void testTwoArgRemoveWriteUnsuccessful() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction(), anyNullaryFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    String toRemove = "foo";
    assertThat(cache.remove(1, toRemove), is(false));
    verify(cache.getCacheLoaderWriter(), never()).delete(1);

  }

  @Test
  public void testTwoArgRemoveThrowsOnCompute() throws Exception {
    String toRemove = "foo";
    when(store.compute(any(Number.class), anyBiFunction(), anyNullaryFunction())).thenThrow(new StoreAccessException("boom"));
    assertThat(cache.remove(1, toRemove), is(false));
    verify(cache.getCacheLoaderWriter(), never()).delete(1);
    verify(store).remove(1);
  }

  @Test(expected=CacheWritingException.class)
  public void testTwoArgRemoveThrowsOnWrite() throws Exception {
    final String expected = "foo";
    when(store.compute(any(Number.class), anyBiFunction(), anyNullaryFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        try {
          function.apply((Number)invocation.getArguments()[0], expected);
        } catch (StorePassThroughException e) {
          throw e.getCause();
        }
        return null;
      }
    });
    doThrow(new Exception()).when(cache.getCacheLoaderWriter()).delete(any(Number.class));
    cache.remove(1, expected);
  }

  @Test
  public void testReplace() throws Exception {
    final String oldValue = "foo";
    final String newValue = "bar";
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, CharSequence, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], oldValue);
        return null;
      }
    });
    when((String)cache.getCacheLoaderWriter().load(any(Number.class))).thenReturn(oldValue);

    assertThat(cache.replace(1, newValue), is(oldValue));
    verify(cache.getCacheLoaderWriter()).write(1, newValue);
  }

  @Test
  public void testReplaceThrowsOnCompute() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenThrow(new StoreAccessException("boom"));
    String value = "foo";
    assertThat(cache.replace(1, value), nullValue());
    verify(cache.getCacheLoaderWriter()).load(1);
    verify(store).remove(1);
  }

  @Test(expected=CacheWritingException.class)
  public void testReplaceThrowsOnWrite() throws Exception {
    final String expected = "old";
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        try {
          function.apply((Number)invocation.getArguments()[0], expected);
        } catch (StorePassThroughException e) {
          throw e.getCause();
        }
        return null;
      }
    });
    when((String)cache.getCacheLoaderWriter().load(any(Number.class))).thenReturn(expected);
    doThrow(new Exception()).when(cache.getCacheLoaderWriter()).write(any(Number.class), anyString());
    cache.replace(1, "bar");
  }

  @Test
  public void testThreeArgReplaceMatch() throws Exception {
    final String cachedValue = "cached";
    final String newValue = "toReplace";

    when(store.compute(any(Number.class), anyBiFunction(), anyNullaryFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], cachedValue);
        return null;
      }
    });

    assertThat(cache.replace(1, cachedValue, newValue), is(true));
    verify(cache.getCacheLoaderWriter()).write(1, newValue);
  }

  @Test
  public void testThreeArgReplaceKeyNotInCache() throws Exception {
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
    verify(cache.getCacheLoaderWriter(), never()).write(1, newValue);
  }

  @Test
  public void testThreeArgReplaceThrowsOnCompute() throws Exception {
    final String oldValue = "cached";
    final String newValue = "toReplace";
    when(store.compute(any(Number.class), anyBiFunction(), anyNullaryFunction())).thenThrow(new StoreAccessException("boom"));

    assertThat(cache.replace(1, oldValue, newValue), is(false));
    verify(cache.getCacheLoaderWriter(), never()).write(1, newValue);
    verify(store).remove(1);
  }

  @Test(expected=CacheWritingException.class)
  public void testThreeArgReplaceThrowsOnWrite() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction(), anyNullaryFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        final String applied;
        try {
          applied = function.apply((Number)invocation.getArguments()[0], "old");
        } catch (StorePassThroughException e) {
          throw e.getCause();
        }

        @SuppressWarnings("unchecked")
        final Store.ValueHolder<Object> mock = mock(Store.ValueHolder.class);

        when(mock.value()).thenReturn(applied);
        return mock;
      }
    });
    doThrow(new Exception()).when(cache.getCacheLoaderWriter()).write(any(Number.class), anyString());
    cache.replace(1, "old", "new");
  }

  @SuppressWarnings("unchecked")
  private static BiFunction<Number, String, String> anyBiFunction() {
    return any(BiFunction.class);
  }

  @SuppressWarnings("unchecked")
  private Function<Number, String> anyFunction() {
    return any(Function.class);
  }

  @SuppressWarnings("unchecked")
  private NullaryFunction<Boolean> anyNullaryFunction() {
    return any(NullaryFunction.class);
  }

  @SuppressWarnings("unchecked")
  private static <A, B, T> BiFunction<A, B, T> asBiFunction(InvocationOnMock in) {
    return (BiFunction<A, B, T>)in.getArguments()[1];
  }

  @SuppressWarnings("unchecked")
  private static <A, T> Function<A, T> asFunction(InvocationOnMock in) {
    return (Function<A, T>)in.getArguments()[1];
  }

}
