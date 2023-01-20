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
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class EvictionEhcacheTest {

  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    cacheManager = builder.build(true);
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testSimplePutWithEviction() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Number.class, CharSequence.class, heap(2))
            .build());

    testCache.put(1, "one");
    testCache.put(2, "two");
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));
    assertThat(testCache.get(2), Matchers.<CharSequence>equalTo("two"));

    testCache.put(3, "three");
    testCache.put(4, "four");

    int count = 0;
    for (@SuppressWarnings("unused") Cache.Entry<Number, CharSequence> entry : testCache) {
      count++;
    }
    assertThat(count, is(2));
  }

  @Test
  public void testSimplePutIfAbsentWithEviction() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Number.class, CharSequence.class, heap(2))
            .build());

    testCache.putIfAbsent(1, "one");
    testCache.putIfAbsent(2, "two");
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));
    assertThat(testCache.get(2), Matchers.<CharSequence>equalTo("two"));

    testCache.putIfAbsent(3, "three");
    testCache.putIfAbsent(4, "four");

    int count = 0;
    for (@SuppressWarnings("unused") Cache.Entry<Number, CharSequence> entry : testCache) {
      count++;
    }
    assertThat(count, is(2));
  }

  @Test
  public void testSimplePutAllWithEviction() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Number.class, CharSequence.class, heap(2))
            .build());

    Map<Integer, String> values = new HashMap<Integer, String>();
    values.put(1, "one");
    values.put(2, "two");
    values.put(3, "three");
    values.put(4, "four");

    testCache.putAll(values);

    int count = 0;
    for (@SuppressWarnings("unused") Cache.Entry<Number, CharSequence> entry : testCache) {
      count++;
    }
    assertThat(count, is(2));
  }

}
