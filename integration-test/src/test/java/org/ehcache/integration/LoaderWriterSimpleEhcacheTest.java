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
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;

import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class LoaderWriterSimpleEhcacheTest {

  private CacheManager cacheManager;
  private Cache<Number, CharSequence> testCache;
  private CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter;

  @Before
  @SuppressWarnings("unchecked")
  public void setUp() throws Exception {
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    cacheLoaderWriter = mock(CacheLoaderWriter.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), org.mockito.ArgumentMatchers.<CacheConfiguration<Number, CharSequence>>any()))
      .thenReturn(CacheLoaderWriter.class.cast(cacheLoaderWriter));
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
  public void testSimplePutIfAbsentWithLoaderAndWriter_absent() throws Exception {
    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.putIfAbsent(1, "one"), is(nullValue()));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verify(cacheLoaderWriter, times(1)).write(eq(1), eq("one"));
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_existsInSor() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> "un");

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.putIfAbsent(1, "one"), Matchers.<CharSequence>equalTo("un"));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("un"));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimplePutIfAbsentWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheLoaderWriter);

    assertThat(testCache.putIfAbsent(1, "one"), Matchers.<CharSequence>equalTo("un"));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("un"));

    verifyZeroInteractions(cacheLoaderWriter);
  }

  @Test
  public void testSimpleReplace2ArgsWithLoaderAndWriter_absent() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> null);

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "one"), is(nullValue()));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  public void testSimpleReplace2ArgsWithLoaderAndWriter_existsInSor() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> "un");

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "one"), Matchers.<CharSequence>equalTo("un"));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verify(cacheLoaderWriter, times(1)).write(eq(1), eq("one"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleReplace2ArgsWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheLoaderWriter);

    assertThat(testCache.replace(1, "one"), Matchers.<CharSequence>equalTo("un"));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verify(cacheLoaderWriter, times(1)).write(eq(1), eq("one"));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  public void testSimpleReplace3ArgsWithLoaderAndWriter_absent() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> null);

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "un", "one"), is(false));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  public void testSimpleReplace3ArgsWithLoaderAndWriter_existsInSor() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> "un");

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "un", "one"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verify(cacheLoaderWriter, times(1)).write(eq(1), eq("one"));
  }

  @Test
  public void testSimpleReplace3ArgsWithLoaderAndWriter_existsInSor_notEquals() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> "un");

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "uno", "one"), is(false));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("un"));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleReplace3ArgsWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheLoaderWriter);

    assertThat(testCache.replace(1, "un", "one"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verify(cacheLoaderWriter, times(1)).write(eq(1), eq("one"));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleReplace3ArgsWithLoaderAndWriter_existsInStore_notEquals() throws Exception {
    testCache.put(1, "un");
    reset(cacheLoaderWriter);

    assertThat(testCache.replace(1, "uno", "one"), is(false));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("un"));

    verifyZeroInteractions(cacheLoaderWriter);
  }

  @Test
  public void testSimpleRemove2ArgsWithLoaderAndWriter_absent() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> null);

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.remove(1, "one"), is(false));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  public void testSimpleRemove2ArgsWithLoaderAndWriter_existsInSor() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> "un");

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.remove(1, "un"), is(true));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verify(cacheLoaderWriter, times(1)).delete(eq(1));
  }

  @Test
  public void testSimpleRemove2ArgsWithLoaderAndWriter_existsInSor_notEquals() throws Exception {
    when(cacheLoaderWriter.load(eq(1))).thenAnswer((Answer) invocation -> "un");

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.remove(1, "one"), is(false));

    verify(cacheLoaderWriter, times(1)).load(eq(1));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleRemove2ArgsWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheLoaderWriter);

    assertThat(testCache.remove(1, "un"), is(true));

    verify(cacheLoaderWriter, times(1)).delete(eq(1));
    verifyNoMoreInteractions(cacheLoaderWriter);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testSimpleRemove2ArgsWithLoaderAndWriter_existsInStore_notEquals() throws Exception {
    testCache.put(1, "un");
    reset(cacheLoaderWriter);

    assertThat(testCache.remove(1, "one"), is(false));

    verifyZeroInteractions(cacheLoaderWriter);
  }

}
