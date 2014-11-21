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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.doThrow;
import static org.junit.Assert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.exceptions.CacheLoaderException;
import org.ehcache.exceptions.CacheWriterException;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.writer.CacheWriter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


/**
 * @author vfunshteyn
 */
public class EhcacheWriterLoaderTest {
  private Ehcache<Number, String> cache;
  private Store<Number, String> store;
   
  @Before
  public void setUp() throws Exception {
    store = mock(Store.class);
    CacheWriter<Number, String> writer = mock(CacheWriter.class);
    CacheLoader<Number, String> loader = mock(CacheLoader.class);
    cache = new Ehcache<Number, String>(
        new CacheConfigurationBuilder().buildConfig(Number.class, String.class), store, loader, writer);
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
    verify(cache.getCacheLoader()).load(1);
  }

  @Test
  public void testGetThrowsOnCompute() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenThrow(new CacheAccessException("boom"));
    String expected = "foo";
    when((String)cache.getCacheLoader().load(any(Number.class))).thenReturn(expected);
    assertThat(cache.get(1), is(expected));
    verify(store).remove(1);
  }
  
  @Test(expected=CacheLoaderException.class)
  public void testGetThrowsOnLoad() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        function.apply((Number)invocation.getArguments()[0]);
        return null;
      }
    });
    when(cache.getCacheLoader().load(any(Number.class))).thenThrow(new Exception());
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
    verify(cache.getCacheWriter()).write(1, "one");
  }

  @Test
  public void testPutThrowsOnCompute() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenThrow(new CacheAccessException("boom"));
    cache.put(1, "one");
    verify(store).remove(1);
    verify(cache.getCacheWriter()).write(1, "one");
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
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    cache.remove(1);
    verify(cache.getCacheWriter()).delete(1);
  }

  @Test
  public void testRemoveThrowsOnCompute() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenThrow(new CacheAccessException("boom"));
    cache.remove(1);
    verify(store).remove(1);
    verify(cache.getCacheWriter()).delete(1);
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
  public void testPutIfAbsent() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function<Number, String> function = asFunction(invocation);
        function.apply((Number)invocation.getArguments()[0]);
        return null;
      }
    });
    cache.putIfAbsent(1, "foo");
    verifyZeroInteractions(cache.getCacheLoader());
    verify(cache.getCacheWriter()).write(1, null, "foo");
  }
  
  @Test
  public void testPutIfAbsentThrowsOnCompute() throws Exception {
    when(store.computeIfAbsent(any(Number.class), anyFunction())).thenThrow(new CacheAccessException("boom"));
    cache.putIfAbsent(1, "one");
    verify(cache.getCacheWriter()).write(1, null, "one");
    verify(store).remove(1);
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
    when(cache.getCacheWriter().write(any(Number.class), anyString(), anyString())).thenThrow(new Exception());
    cache.putIfAbsent(1, "one");
  }
  
  @Test
  public void testTwoArgRemoveMatch() throws Exception {
    final String cachedValue = "cached";
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], cachedValue);
        return null;
      }
    });
    when(cache.getCacheWriter().delete(any(Number.class), anyString())).thenReturn(true);
    assertThat(cache.remove(1, cachedValue), is(true));
    verify(cache.getCacheWriter()).delete(1, cachedValue);
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
    when(cache.getCacheWriter().delete(any(Number.class), anyString())).thenReturn(true);
    assertThat(cache.remove(1, toRemove), is(true));
    verify(cache.getCacheWriter()).delete(1, toRemove);
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
    when(cache.getCacheWriter().delete(any(Number.class), anyString())).thenReturn(false);
    assertThat(cache.remove(1, toRemove), is(false));
    verify(cache.getCacheWriter()).delete(1, toRemove);
    
  }

  @Test
  public void testTwoArgRemoveThrowsOnCompute() throws Exception {
    String toRemove = "foo";
    when(store.compute(any(Number.class), anyBiFunction())).thenThrow(new CacheAccessException("boom"));
    when(cache.getCacheWriter().delete(any(Number.class), anyString())).thenReturn(true);
    assertThat(cache.remove(1, toRemove), is(true));
    verify(cache.getCacheWriter()).delete(1, toRemove);
    verify(store).remove(1);
  }
  
  @Test(expected=CacheWriterException.class)
  public void testTwoArgRemoveThrowsOnWrite() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    when(cache.getCacheWriter().delete(any(Number.class), anyString())).thenThrow(new Exception());
    cache.remove(1, "foo");
  }
  
  @Test
  public void testReplace() throws Exception {
    final String oldValue = "foo";
    final String newValue = "bar";
    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, CharSequence, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], oldValue);
        return null;
      }
    });
    when((String)cache.getCacheLoader().load(any(Number.class))).thenReturn(oldValue);

    assertThat(cache.replace(1, newValue), is(oldValue));
    verify(cache.getCacheWriter()).write(1, newValue);
  }
  
  @Test
  public void testReplaceThrowsOnCompute() throws Exception {
    when(store.computeIfPresent(any(Number.class), anyBiFunction())).thenThrow(new CacheAccessException("boom"));
    String value = "foo";
    assertThat(cache.replace(1, value), nullValue());
    verify(cache.getCacheWriter()).write(1, value);
    verify(store).remove(1);
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
    when((String)cache.getCacheLoader().load(any(Number.class))).thenReturn(expected);
    doThrow(new Exception()).when(cache.getCacheWriter()).write(any(Number.class), anyString());
    cache.replace(1, "bar");
  }
  
  @Test
  public void testThreeArgReplaceMatch() throws Exception {
    final String cachedValue = "cached";
    final String newValue = "toReplace";

    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], cachedValue);
        return null;
      }
    });
    when(cache.getCacheWriter().write(any(Number.class), anyString(), anyString())).thenReturn(true);
    
    assertThat(cache.replace(1, cachedValue, newValue), is(true));
    verify(cache.getCacheWriter()).write(1, cachedValue, newValue);
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
    when(cache.getCacheWriter().write(any(Number.class), anyString(), anyString())).thenReturn(true);
    
    assertThat(cache.replace(1, oldValue, newValue), is(true));
    verify(cache.getCacheWriter()).write(1, oldValue, newValue);
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
    when(cache.getCacheWriter().write(any(Number.class), anyString(), anyString())).thenReturn(false);
    
    assertThat(cache.replace(1, oldValue, newValue), is(false));
    verify(cache.getCacheWriter()).write(1, oldValue, newValue);
  }

  @Test
  public void testThreeArgReplaceThrowsOnCompute() throws Exception {
    final String oldValue = "cached";
    final String newValue = "toReplace";
    when(store.compute(any(Number.class), anyBiFunction())).thenThrow(new CacheAccessException("boom"));
    when(cache.getCacheWriter().write(any(Number.class), anyString(), anyString())).thenReturn(true);
    
    assertThat(cache.replace(1, oldValue, newValue), is(true));
    verify(cache.getCacheWriter()).write(1, oldValue, newValue);
    verify(store).remove(1);
  }
  
  @Test(expected=CacheWriterException.class)
  public void testThreeArgReplaceThrowsOnWrite() throws Exception {
    when(store.compute(any(Number.class), anyBiFunction())).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        BiFunction<Number, String, String> function = asBiFunction(invocation);
        function.apply((Number)invocation.getArguments()[0], null);
        return null;
      }
    });
    when(cache.getCacheWriter().write(any(Number.class), anyString(), anyString())).thenThrow(new Exception());
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
  private static <A, B, T> BiFunction<A, B, T> asBiFunction(InvocationOnMock in) {
    return (BiFunction<A, B, T>)in.getArguments()[1];
  }
  
  @SuppressWarnings("unchecked")
  private static <A, T> Function<A, T> asFunction(InvocationOnMock in) {
    return (Function<A, T>)in.getArguments()[1];
  }

}
