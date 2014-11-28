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
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class LoaderSimpleEhcacheTest {

  private CacheManager cacheManager;
  private Cache<Number, CharSequence> testCache;
  private CacheLoader<? super Number, ? super CharSequence> cacheLoader;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheLoaderFactory cacheLoaderFactory = mock(CacheLoaderFactory.class);
    cacheLoader = mock(CacheLoader.class);
    when(cacheLoaderFactory.createCacheLoader(anyString(), (CacheConfiguration<Number, CharSequence>) anyObject())).thenReturn((CacheLoader) cacheLoader);
    builder.using(cacheLoaderFactory);
    cacheManager = builder.build();
    testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testSimpleGetWithLoader() throws Exception {
    when(cacheLoader.load(eq(1))).thenReturn("one");

    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));
    assertThat(testCache.get(2), is(nullValue()));

    verify(cacheLoader, times(1)).load(eq(1));
    verify(cacheLoader, times(1)).load(eq(2));
  }

  @Test
  @Ignore("getAll throws NPE")
  public void testSimpleGetAllWithLoader() throws Exception {
    when(cacheLoader.loadAll((Iterable)any())).thenAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Iterable<Integer> iterable = (Iterable) invocation.getArguments()[0];
        Map<Number, CharSequence> result = new HashMap<Number, CharSequence>();
        for (Integer i : iterable) {
          switch (i) {
            case 1:
              result.put(1, "one");
              break;
            case 2:
              result.put(2, "two");
              break;
            case 3:
              result.put(3, null);
              break;
            default:
              throw new AssertionError("Should not try to load key " + i);
          }
        }

        return result;
      }
    });

    Map<Number, CharSequence> all = testCache.getAll(Arrays.asList(1, 2, 3));
    assertThat(all.size(), is(3));
    assertThat(all.get(1), Matchers.<CharSequence>equalTo("one"));
    assertThat(all.get(2), Matchers.<CharSequence>equalTo("two"));
    assertThat(all.get(3), is(Matchers.nullValue()));
  }

}
