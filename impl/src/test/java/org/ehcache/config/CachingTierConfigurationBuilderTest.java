/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config;

import org.ehcache.Caching;
import org.junit.Test;
import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;

import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.internal.util.Matchers.hasKey;

/**
 * @author Alex Snaps
 */
public class CachingTierConfigurationBuilderTest {

  @Test
  public void testNothing() throws CacheAccessException {

    final Cache<String, String> cache = Caching.createCache(
        newCacheConfigurationBuilder()
            .buildCacheConfig(String.class, String.class));

    assertThat(cache, not(hasKey("key")));
    assertThat(cache.getAndPut("key", "value"), nullValue());
    assertThat(cache, hasKey("key"));
  }
}
