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
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.service.EhcacheServiceConfiguration;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.spi.writer.CacheWriterFactory;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class LoaderWriterSimple107CompliantEhcacheTest {

  private CacheManager cacheManager;
  private Cache<Number, CharSequence> testCache;
  private CacheWriter<? super Number, ? super CharSequence> cacheWriter;
  private CacheLoader<? super Number, ? super CharSequence> cacheLoader;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheLoaderFactory cacheLoaderFactory = mock(CacheLoaderFactory.class);
    cacheLoader = mock(CacheLoader.class);
    when(cacheLoaderFactory.createCacheLoader(anyString(), (CacheConfiguration<Number, CharSequence>) anyObject())).thenReturn((CacheLoader) cacheLoader);
    builder.using(cacheLoaderFactory);
    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    cacheWriter = mock(CacheWriter.class);
    when(cacheWriterFactory.createCacheWriter(anyString(), (CacheConfiguration<Number, CharSequence>) anyObject())).thenReturn((CacheWriter) cacheWriter);
    builder.using(cacheWriterFactory);
    cacheManager = builder.build();
    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().addServiceConfig(new EhcacheServiceConfiguration().noLoadInAtomics(true)).buildConfig(Number.class, CharSequence.class));
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

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).write(eq(1), eq("one"));
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_existsInSor() throws Exception {
    when(cacheLoader.load(eq(1))).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        return "un";
      }
    });

    assertThat(testCache.containsKey(1), is(false));
    assertThat(testCache.putIfAbsent(1, "one"), is(nullValue()));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));

    verifyZeroInteractions(cacheLoader);
    verify(cacheWriter, times(1)).write(eq(1), eq("one"));
  }

  @Test
  public void testSimplePutIfAbsentWithLoaderAndWriter_existsInStore() throws Exception {
    testCache.put(1, "un");
    reset(cacheWriter);

    assertThat(testCache.putIfAbsent(1, "one"), Matchers.<CharSequence>equalTo("un"));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("un"));

    verifyZeroInteractions(cacheLoader);
    verifyZeroInteractions(cacheWriter);
  }

}
