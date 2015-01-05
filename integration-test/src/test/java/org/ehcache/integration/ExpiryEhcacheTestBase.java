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
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.TimeSourceConfiguration;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public abstract class ExpiryEhcacheTestBase {

  private CacheManager cacheManager;
  private Cache<Number, CharSequence> testCache;
  private final ManualTimeSource manualTimeSource = new ManualTimeSource();

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    cacheManager = builder.build();
    CacheConfigurationBuilder<Object, Object> objectObjectCacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .addServiceConfig(new TimeSourceConfiguration(manualTimeSource))
        .withExpiry(Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.SECONDS)));
    testCache = cacheManager.createCache("testCache", objectObjectCacheConfigurationBuilder.buildConfig(Number.class, CharSequence.class));
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testSimplePutWithExpiry() throws Exception {
    insert(testCache, getEntries());

    assertThat(cacheSize(testCache), is(2));
    manualTimeSource.setTime(1001);
    assertThat(cacheSize(testCache), is(0));
  }

  @Test
  public void testSimplePutWithExpiry_get() throws Exception {
    insert(testCache, getEntries());

    assertThat(cacheSize(testCache), is(2));
    manualTimeSource.setTime(1001);
    assertThat(testCache.get(1), is(nullValue()));
    assertThat(testCache.get(2), is(nullValue()));
  }

  @Test
  public void testSimplePutWithExpiry_getAll() throws Exception {
    insert(testCache, getEntries());

    assertThat(cacheSize(testCache), is(2));
    manualTimeSource.setTime(1001);
    assertThat(testCache.getAll(new HashSet<Number>(Arrays.asList(1, 2))).size(), is(2));
  }

  @Test
  public void testSimplePutWithExpiry_putIfAbsent() throws Exception {
    insert(testCache, getEntries());

    assertThat(cacheSize(testCache), is(2));
    manualTimeSource.setTime(1001);
    assertThat(testCache.putIfAbsent(1, "one#2"), is(nullValue()));
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one#2"));
    assertThat(testCache.putIfAbsent(2, "two#2"), is(nullValue()));
    assertThat(testCache.get(2), Matchers.<CharSequence>equalTo("two#2"));
  }

  @Test
  public void testSimplePutWithExpiry_remove2Args() throws Exception {
    insert(testCache, getEntries());

    assertThat(cacheSize(testCache), is(2));
    manualTimeSource.setTime(1001);
    assertThat(testCache.remove(1, "one"), is(false));
    assertThat(testCache.get(1), is(nullValue()));
    assertThat(testCache.remove(2, "two"), is(false));
    assertThat(testCache.get(2), is(nullValue()));
  }

  @Test
  public void testSimplePutWithExpiry_replace2Args() throws Exception {
    insert(testCache, getEntries());

    assertThat(cacheSize(testCache), is(2));
    manualTimeSource.setTime(1001);
    assertThat(testCache.replace(1, "one#2"), is(nullValue()));
    assertThat(testCache.get(1), is(nullValue()));
    assertThat(testCache.replace(2, "two#2"), is(nullValue()));
    assertThat(testCache.get(2), is(nullValue()));
  }

  @Test
  public void testSimplePutWithExpiry_replace3Args() throws Exception {
    insert(testCache, getEntries());

    assertThat(cacheSize(testCache), is(2));
    manualTimeSource.setTime(1001);
    assertThat(testCache.replace(1, "one", "one#2"), is(false));
    assertThat(testCache.get(1), is(nullValue()));
    assertThat(testCache.replace(2, "two", "two#2"), is(false));
    assertThat(testCache.get(2), is(nullValue()));
  }

  protected abstract void insert(Cache<Number, CharSequence> testCache, Map<Number, CharSequence> entries);

  private Map<Number, CharSequence> getEntries() {
    HashMap<Number, CharSequence> result = new HashMap<Number, CharSequence>();
    result.put(1, "one");
    result.put(2, "two");
    return result;
  }

  private static int cacheSize(Cache<?, ?> cache) {
    int count = 0;
    for (@SuppressWarnings("unused") Cache.Entry<?, ?> entry : cache) {
      count++;
    }
    return count;
  }

  static class ManualTimeSource implements TimeSource {
    private long time;

    public void setTime(long time) {
      this.time = time;
    }

    @Override
    public long getTimeMillis() {
      return time;
    }
  }

}

