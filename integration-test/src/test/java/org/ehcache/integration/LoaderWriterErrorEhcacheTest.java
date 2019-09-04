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
package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class LoaderWriterErrorEhcacheTest {

  private CacheManager cacheManager;
  private Cache<Number, CharSequence> testCache;
  private CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter;

  @SuppressWarnings("unchecked")
  @Before
  public void setUp() throws Exception {
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    cacheLoaderWriter = mock(CacheLoaderWriter.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<Number, CharSequence>) any())).thenReturn((CacheLoaderWriter) cacheLoaderWriter);
    cacheManager = newCacheManagerBuilder().using(cacheLoaderWriterProvider).build(true);
    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Number.class, CharSequence.class, heap(10))
            .withLoaderWriter(cacheLoaderWriter)
            .build());
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testGetWithLoaderException() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenThrow(new Exception("TestException: cannot load data"));

    try {
      testCache.get(1);
      fail("expected CacheLoadingException");
    } catch (CacheLoadingException ex) {
      // expected
    }

    verify(cacheLoaderWriter, times(1)).load(eq(1));
  }

  @Test
  public void testGetAllWithLoaderException() throws Exception {
    when(cacheLoaderWriter.loadAll(ArgumentMatchers.<Iterable<Number>>any())).thenAnswer(invocation -> {
      @SuppressWarnings("unchecked")
      Iterable<Integer> iterable = (Iterable<Integer>) invocation.getArguments()[0];

      Map<Number, CharSequence> result = new HashMap<>();

      for (int i : iterable) {
        switch (i) {
          case 1:
            result.put(1, "one");
            break;
          case 2:
            throw new Exception("Mock Exception: cannot load 2");
          case 3:
            result.put(3, "three");
            break;
          case 4:
            result.put(4, null);
            break;
          default:
            throw new AssertionError("should not try to load key " + i);
        }
      }

      return result;
    });

    try {
      testCache.getAll(new HashSet<Number>(Arrays.asList(1, 2, 3, 4)));
      fail("expected BulkCacheLoadingException");
    } catch (BulkCacheLoadingException ex) {
      assertThat(ex.getFailures().size(), is(1));
      assertThat(ex.getFailures().get(2), is(notNullValue()));
      assertThat(ex.getSuccesses().size(), is(lessThan(4)));
      assertThat(ex.getSuccesses().containsKey(2), is(false));
    }
  }

  @Test
  public void testPutWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).write(eq(1), eq("one"));

    try {
      testCache.put(1, "one");
      fail("expected CacheWritingException");
    } catch (CacheWritingException ex) {
      // expected
    }
  }

  @Test
  public void testRemoveWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).delete(eq(1));

    try {
      testCache.remove(1);
      fail("expected CacheWritingException");
    } catch (CacheWritingException ex) {
      // expected
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testRemoveAllWithWriterException() throws Exception {
    doAnswer(invocation -> {
      Iterable<Integer> iterable = (Iterable) invocation.getArguments()[0];
      Set<Integer> result = new HashSet<>();

      for (Integer i : iterable) {
        switch (i) {
          case 2:
            throw new Exception("Mock Exception: cannot write 2");
          case 1:
          case 3:
          case 4:
            result.add(i);
            break;
          default:
            throw new AssertionError("should not try to delete key " + i);
        }
      }

      return result;
    }).when(cacheLoaderWriter).deleteAll(ArgumentMatchers.<Iterable>any());

    try {
      testCache.removeAll(new HashSet<Number>(Arrays.asList(1, 2, 3, 4)));
      fail("expected CacheWritingException");
    } catch (BulkCacheWritingException ex) {
      assertThat(ex.getFailures().size(), is(1));
      assertThat(ex.getFailures().get(2), is(notNullValue()));
      assertThat(ex.getSuccesses().size(), is(3));
      assertThat(ex.getSuccesses().containsAll(Arrays.asList(1, 3, 4)), is(true));
    }
  }

  @Test
  public void testRemove2ArgsWithNoCacheEntry_should_not_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).delete(eq(1));

    testCache.remove(1, "one");
  }

  @Test
  public void testRemove2ArgsWithNotMatchingCacheEntry_should_not_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).delete(eq(1));

    testCache.put(1, "un");
    testCache.remove(1, "one");
  }

  @Test
  public void testRemove2ArgsWithWriterException_should_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).delete(eq(1));

    testCache.put(1, "one");
    try {
      testCache.remove(1, "one");
      fail("expected CacheWritingException");
    } catch (CacheWritingException ex) {
      // expected
    }
  }

  @Test
  public void testReplace2ArgsWithWriterException_should_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).write(eq(1), eq("one#2"));

    testCache.put(1, "one");
    try {
      testCache.replace(1, "one#2");
      fail("expected CacheWritingException");
    } catch (CacheWritingException ex) {
      // expected
    }
  }

  @Test
  public void testReplace2ArgsWithNoCacheEntry_should_not_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).write(eq(1), eq("one#2"));

    testCache.replace(1, "one#2");
  }

  @Test
  public void testReplace3ArgsWithWriterException_should_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).write(eq(1), eq("one#2"));

    testCache.put(1, "one");
    try {
      testCache.replace(1, "one", "one#2");
      fail("expected CacheWritingException");
    } catch (CacheWritingException ex) {
      // expected
    }
  }

  @Test
  public void testReplace3ArgsWithNotMatchingCacheEntry_should_not_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).write(eq(1), eq("one#2"));

    testCache.put(1, "un");
    testCache.replace(1, "one", "one#2");
  }

  @Test
  public void testReplace3ArgsWithNoCacheEntry_should_not_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).write(eq(1), eq("one#2"));

    testCache.replace(1, "one", "one#2");
  }

  @Test
  public void testPutIfAbsentWithWriterException_should_call_writer() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).write(eq(1), eq("one"));

    try {
      testCache.putIfAbsent(1, "one");
      fail("expected CacheWritingException");
    } catch (CacheWritingException ex) {
      // expected
    }

    testCache.put(2, "two");
    testCache.putIfAbsent(2, "two#2");
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
  public void testPutAllWithWriterException() throws Exception {
    doThrow(new Exception("Mock Exception: cannot write 1")).when(cacheLoaderWriter).writeAll(ArgumentMatchers.<Iterable>any());

    Map<Integer, String> values = new HashMap<>();
    values.put(1, "one");
    values.put(2, "two");

    try {
      testCache.putAll(values);
      fail("expected CacheWritingException");
    } catch (CacheWritingException ex) {
      // expected
    }
  }
}
