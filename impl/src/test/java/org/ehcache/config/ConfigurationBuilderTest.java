/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config;

import org.junit.Test;
import org.ehcache.CacheManager;
import org.ehcache.spi.Service;
import org.ehcache.spi.ServiceConfiguration;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.ehcache.config.ConfigurationBuilder.newConfigurationBuilder;
import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;

/**
 * @author Alex Snaps
 */
public class ConfigurationBuilderTest {

  @Test
  public void testNothing() throws InterruptedException {
    final Configuration configuration = newConfigurationBuilder()
        .addCache("foo", newCacheConfigurationBuilder()
            .buildCacheConfig(String.class, String.class))
            // TODO UNCOMMENT AND FIX
//        .addCache("atomicFoo", newCacheConfigurationBuilder()
//            .buildAtomicCacheConfig(String.class, String.class))
        .addService(new ServiceConfiguration<Service>() {
          @Override
          public Class<Service> getServiceType() {
            return Service.class;
          }
        })
        .build();
    CacheManager cacheManager = new CacheManager(configuration);
    assertThat(cacheManager.getCache("bar", String.class, String.class), nullValue());
    assertThat(cacheManager.getCache("foo", String.class, String.class), not(nullValue()));
    // TODO UNCOMMENT AND FIX
//    assertThat(cacheManager.getCache("atomicFoo", String.class, String.class), not(nullValue()));
  }
}
