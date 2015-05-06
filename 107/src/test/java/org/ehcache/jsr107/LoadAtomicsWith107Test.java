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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * LoadAtomicsWith107Test
 */
public class LoadAtomicsWith107Test {
  private CacheLoader<Number, CharSequence> cacheLoader;
  private CacheWriter<Number, CharSequence> cacheWriter;
  private Cache<Number, CharSequence> testCache;
  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    cacheLoader = mock(CacheLoader.class);
    cacheWriter = mock(CacheWriter.class);

    CachingProvider provider = Caching.getCachingProvider();
    cacheManager = provider.getCacheManager(this.getClass().getResource("/ehcache-loader-writer-107-load-atomics.xml").toURI(), getClass().getClassLoader());

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

    verify(cacheLoader).load(1);
    verify(cacheWriter, times(1)).write(eq(new Eh107CacheLoaderWriter.Entry<Number, CharSequence>(1, "one")));
  }

}
