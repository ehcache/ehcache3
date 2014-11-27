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
import org.ehcache.config.CacheConfigurationBuilder;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class SimpleEhcacheTest {

  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    cacheManager = builder.build();
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testSimplePut() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));
  }

  @Test
  public void testSimplePutIfAbsent() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    CharSequence one = testCache.putIfAbsent(1, "one");
    assertThat(one, is(nullValue()));
    CharSequence one_2 = testCache.putIfAbsent(1, "one#2");
    assertThat(one_2, Matchers.<CharSequence>equalTo("one"));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));
  }

  @Test
  public void testSimplePutAll() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    Map<Integer, String> values = new HashMap<Integer, String>();
    values.put(1, "one");
    values.put(2, "two");
    values.put(3, "three");

    testCache.putAll(values.entrySet());

    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));
    assertThat(testCache.get(2), Matchers.<CharSequence>equalTo("two"));
    assertThat(testCache.get(3), Matchers.<CharSequence>equalTo("three"));
  }

  @Test
  @Ignore("getAll throws NoSuchElementException")
  public void testSimpleGetAll() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");
    testCache.put(2, "two");

    Map<Number, CharSequence> all = testCache.getAll(Arrays.asList(1, 2, 3));
    assertThat(all.size(), is(3));
    assertThat(all.get(1), Matchers.<CharSequence>equalTo("one"));
    assertThat(all.get(2), Matchers.<CharSequence>equalTo("two"));
    assertThat(all.get(2), is(nullValue()));
  }

  @Test
  public void testSimpleContainsKey() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");

    assertThat(testCache.containsKey(1), is(true));
    assertThat(testCache.containsKey(2), is(false));
  }

  @Test
  public void testClear() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");
    testCache.put(2, "two");

    testCache.clear();

    assertThat(testCache.get(1), is(nullValue()));
    assertThat(testCache.get(2), is(nullValue()));
  }

  @Test
  public void testSimpleRemove() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");
    testCache.put(2, "two");

    testCache.remove(1);

    assertThat(testCache.get(1), is(nullValue()));
    assertThat(testCache.get(2), is(notNullValue()));
  }

  @Test
  public void testSimpleRemoveAll() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");
    testCache.put(2, "two");
    testCache.put(3, "three");

    testCache.removeAll(Arrays.asList(1, 2));

    assertThat(testCache.get(1), is(nullValue()));
    assertThat(testCache.get(2), is(nullValue()));
    assertThat(testCache.get(3), is(notNullValue()));
  }

  @Test
  public void testSimpleRemove2Args() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");

    assertThat(testCache.remove(1, "one_"), is(false));
    assertThat(testCache.get(1), is(notNullValue()));
    assertThat(testCache.remove(1, "one"), is(true));
    assertThat(testCache.get(1), is(nullValue()));
  }

  @Test
  public void testSimpleReplace() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");

    assertThat(testCache.replace(1, "one_"), Matchers.<CharSequence>equalTo("one"));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one_"));
    assertThat(testCache.replace(2, "two_"), is(nullValue()));
  }

  @Test
  public void testSimpleReplace3Args() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");

    assertThat(testCache.replace(1, "one_", "one@"), is(false));
    assertThat(testCache.replace(1, "one", "one#"), is(true));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one#"));
    assertThat(testCache.replace(2, "two", "two#"), is(false));
  }

}
