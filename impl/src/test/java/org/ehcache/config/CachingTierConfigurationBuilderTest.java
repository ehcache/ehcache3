/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config;

import org.ehcache.internal.HeapResource;
import org.ehcache.spi.cache.CacheProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.ServiceProvider;
import org.junit.Test;
import org.ehcache.Cache;
import org.ehcache.exceptions.CacheAccessException;

import java.util.Collection;

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

    final ServiceProvider serviceProvider = new ServiceProvider();
    final CacheConfiguration<String, String> config = newCacheConfigurationBuilder()
    .buildCacheConfig(String.class, String.class);
    CacheProvider cacheProvider = serviceProvider.findService(HeapResource.class);
    Class<String> keyType = config.getKeyType();
    Class<String> valueType = config.getValueType();
    Collection<ServiceConfiguration<?>> serviceConfigs = config.getServiceConfigurations();
    ServiceConfiguration<?>[] serviceConfigArray = serviceConfigs.toArray(new ServiceConfiguration[serviceConfigs.size()]);
    final Cache<String, String> cache = cacheProvider.createCache(keyType, valueType, serviceProvider, serviceConfigArray);

    assertThat(cache, not(hasKey("key")));
    assertThat(cache.getAndPut("key", "value"), nullValue());
    assertThat(cache, hasKey("key"));
  }
}
