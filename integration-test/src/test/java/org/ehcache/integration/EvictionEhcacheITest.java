package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfigurationBuilder;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

/**
 * @author Ludovic Orban
 */
public class EvictionEhcacheITest {

  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    CacheManagerBuilder<CacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder();
    cacheManager = builder.build();
  }

  @After
  public void tearDown() throws Exception {
    if (cacheManager != null) {
//      cacheManager.close();
    }
  }

  @Test
  public void testSimplePutWithEviction() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().maxEntriesInCache(2).buildConfig(Number.class, CharSequence.class));

    testCache.put(1, "one");
    testCache.put(2, "two");
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));
    assertThat(testCache.get(2), Matchers.<CharSequence>equalTo("two"));

    testCache.put(3, "three");
    testCache.put(4, "four");

    int count = 0;
    for (Cache.Entry<Number, CharSequence> entry : testCache) {
      count++;
    }
    assertThat(count, is(2));
  }

  @Test
  public void testSimplePutIfAbsentWithEviction() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().maxEntriesInCache(2).buildConfig(Number.class, CharSequence.class));

    testCache.putIfAbsent(1, "one");
    testCache.putIfAbsent(2, "two");
    assertThat(testCache.get(1), Matchers.<CharSequence>equalTo("one"));
    assertThat(testCache.get(2), Matchers.<CharSequence>equalTo("two"));

    testCache.putIfAbsent(3, "three");
    testCache.putIfAbsent(4, "four");

    int count = 0;
    for (Cache.Entry<Number, CharSequence> entry : testCache) {
      count++;
    }
    assertThat(count, is(2));
  }

  @Test
  public void testSimplePutAllWithEviction() throws Exception {
    Cache<Number, CharSequence> testCache = cacheManager.createCache("testCache", CacheConfigurationBuilder.newCacheConfigurationBuilder().maxEntriesInCache(2).buildConfig(Number.class, CharSequence.class));

    Map<Integer, String> values = new HashMap<Integer, String>();
    values.put(1, "one");
    values.put(2, "two");
    values.put(3, "three");
    values.put(4, "four");

    testCache.putAll(values.entrySet());

    int count = 0;
    for (Cache.Entry<Number, CharSequence> entry : testCache) {
      count++;
    }
    assertThat(count, is(2));
  }

}
