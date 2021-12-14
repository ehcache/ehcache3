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

package org.ehcache.jsr107;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class LoaderWriterTest {

  @Mock
  private CacheLoader<Number, CharSequence> cacheLoader;
  @Mock
  private CacheWriter<Number, CharSequence> cacheWriter;
  private Cache<Number, CharSequence> testCache;
  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    CachingProvider provider = Caching.getCachingProvider();
    cacheManager = provider.getCacheManager(this.getClass().getResource("/ehcache-loader-writer-107.xml").toURI(), getClass().getClassLoader());

    testCache = cacheManager.createCache("testCache", new MutableConfiguration<Number, CharSequence>()
        .setReadThrough(true)
        .setWriteThrough(true)
        .setCacheLoaderFactory(new Factory<CacheLoader<Number, CharSequence>>() {
          @Override
          public CacheLoader<Number, CharSequence> create() {
            return cacheLoader;
          }
        })
        .setCacheWriterFactory(new Factory<CacheWriter<? super Number, ? super CharSequence>>() {
          @Override
          public CacheWriter<? super Number, ? super CharSequence> create() {
            return cacheWriter;
          }
        })
        .setTypes(Number.class, CharSequence.class));
  }

  @After
  public void tearDown() throws Exception {
    cacheManager.close();
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_absent() throws Exception {
    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.putIfAbsent(1, "one"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).write(eq(new Eh107CacheLoaderWriter.Entry<Number, CharSequence>(1, "one")));
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_existsInSor() throws Exception {
    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.putIfAbsent(1, "one"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).write(eq(new Eh107CacheLoaderWriter.Entry<Number, CharSequence>(1, "one")));
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheWriter);

    assertThat(testCache.putIfAbsent(1, "one"), is(false));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("un"));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }


  @Test
  public void testSimpleReplace2ArgsWithLoaderAndWriter_absent() throws Exception {
    when(cacheLoader.load(eq(1))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return null;
      }
    });

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "one"), is(false));
    assertThat(testCache.containsKey(1), is(false));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

  @Test
  public void testSimpleReplace2ArgsWithLoaderAndWriter_existsInSor() throws Exception {
    when(cacheLoader.load(eq(1))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return "un";
      }
    });

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "one"), is(false));
    assertThat(testCache.containsKey(1), is(false));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

  @Test
  public void testSimpleReplace2ArgsWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheWriter);

    assertThat(testCache.replace(1, "one"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).write(new Eh107CacheLoaderWriter.Entry<Number, CharSequence>(1, "one"));
  }

  @Test
  public void testSimpleReplace3ArgsWithLoaderAndWriter_absent() throws Exception {
    when(cacheLoader.load(eq(1))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return null;
      }
    });

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "un", "one"), is(false));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

  @Test
  public void testSimpleReplace3ArgsWithLoaderAndWriter_existsInSor() throws Exception {
    when(cacheLoader.load(eq(1))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return "un";
      }
    });

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.replace(1, "un", "one"), is(false));
    assertThat(testCache.containsKey(1), is(false));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

  @Test
  public void testSimpleReplace3ArgsWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheWriter);

    assertThat(testCache.replace(1, "un", "one"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).write(new Eh107CacheLoaderWriter.Entry<Number, CharSequence>(1, "one"));
  }

  @Test
  public void testSimpleReplace3ArgsWithLoaderAndWriter_existsInStore_notEquals() throws Exception {
    testCache.put(1, "un");
    reset(cacheWriter);

    assertThat(testCache.replace(1, "uno", "one"), is(false));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("un"));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

  @Test
  public void testSimpleRemove2ArgsWithLoaderAndWriter_absent() throws Exception {
    when(cacheLoader.load(eq(1))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return null;
      }
    });

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.remove(1, "one"), is(false));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

  @Test
  public void testSimpleRemove2ArgsWithLoaderAndWriter_existsInSor() throws Exception {
    when(cacheLoader.load(eq(1))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return "un";
      }
    });

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.remove(1, "un"), is(false));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

  @Test
  public void testSimpleRemove2ArgsWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheWriter);

    assertThat(testCache.remove(1, "un"), is(true));

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).delete(eq(1));
  }

  @Test
  public void testSimpleRemove2ArgsWithLoaderAndWriter_existsInStore_notEquals() throws Exception {
    testCache.put(1, "un");
    reset(cacheWriter);

    assertThat(testCache.remove(1, "one"), is(false));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

  private void reset(Object mock) {
    Mockito.reset(mock);
  }

}
