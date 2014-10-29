package org.ehcache;

import org.ehcache.internal.store.service.OnHeapStoreServiceConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

/**
 * @author Ludovic Orban
 */
public class LudoTest {

  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    cacheManager = newCacheManagerBuilder().build();
  }

  @After
  public void tearDown() throws Exception {
    cacheManager.close();
  }

  @Test
  public void testName() throws Exception {
    final Cache<Long, String> cache1 = cacheManager.createCache("cache1",
            newCacheConfigurationBuilder().buildCacheConfig(Long.class, String.class));
    performAssertions(cache1, true);

    final Cache<Long, String> cache2 = cacheManager.createCache("cache2",
      newCacheConfigurationBuilder().addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(true)).buildCacheConfig(Long.class, String.class));
    performAssertions(cache2, false);

    final Cache<Long, String> cache3 = cacheManager.createCache("cache3",
      newCacheConfigurationBuilder().addServiceConfig(new OnHeapStoreServiceConfig().storeByValue(false)).buildCacheConfig(Long.class, String.class));
    performAssertions(cache3, true);
  }

  private void performAssertions(Cache<Long, String> cache, boolean same) {
    cache.put(1L, "one");
    String s1 = cache.get(1L);
    String s2 = cache.get(1L);
    String s3 = cache.get(1L);

    assertThat(s1 == s2, is(same));
    assertThat(s2 == s3, is(same));
  }
}
