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

package org.ehcache.docs;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.jsr107.Eh107Configuration;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.spi.CachingProvider;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

/**
 * ProviderConfigurationWrapperTest
 */
public class EhCache107ConfigurationIntegrationTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhCache107ConfigurationIntegrationTest.class);

  private CacheManager cacheManager;
  private CachingProvider cachingProvider;

  @Before
  public void setUp() throws Exception {
    cachingProvider = Caching.getCachingProvider();
    cacheManager = cachingProvider.getCacheManager();
  }

  @Test
  public void testEhcacheReturnsCompleteConfigurationWhenNotPassedIn() {
    Cache<Long, String> cache = cacheManager.createCache("cacheWithoutCompleteConfig", new Configuration<Long, String>() {
      @Override
      public Class<Long> getKeyType() {
        return Long.class;
      }

      @Override
      public Class<String> getValueType() {
        return String.class;
      }

      @Override
      public boolean isStoreByValue() {
        return true;
      }
    });

    CompleteConfiguration<Long, String> configuration = cache.getConfiguration(CompleteConfiguration.class);
    assertThat(configuration, notNullValue());
    assertThat(configuration.isStoreByValue(), is(true));

    // Respects defaults
    assertThat(configuration.getExpiryPolicyFactory(), equalTo(EternalExpiryPolicy.factoryOf()));
  }

  @Test
  public void testGettingToEhcacheConfiguration() {
    // tag::mutableConfigurationExample[]
    MutableConfiguration<Long, String> configuration = new MutableConfiguration<Long, String>();
    configuration.setTypes(Long.class, String.class);
    Cache<Long, String> cache = cacheManager.createCache("someCache", configuration); //// <1>

    assertThat(cache.getConfiguration(CompleteConfiguration.class), notNullValue()); //// <2>

    Eh107Configuration<Long, String> eh107Configuration = cache.getConfiguration(Eh107Configuration.class); //// <3>

    CacheRuntimeConfiguration<Long, String> runtimeConfiguration = eh107Configuration.unwrap(CacheRuntimeConfiguration.class); //// <4>
    assertThat(runtimeConfiguration, notNullValue());
    // end::mutableConfigurationExample[]

    // Check uses default JSR-107 expiry
    long nanoTime = System.nanoTime();
    LOGGER.info("Seeding random with {}", nanoTime);
    Random random = new Random(nanoTime);
    assertThat(runtimeConfiguration.getExpiry().getExpiryForCreation(random.nextLong(), Long.toOctalString(random.nextLong())),
                equalTo(Duration.FOREVER));
    assertThat(runtimeConfiguration.getExpiry().getExpiryForAccess(random.nextLong(), Long.toOctalString(random.nextLong())),
                nullValue());
    assertThat(runtimeConfiguration.getExpiry().getExpiryForUpdate(random.nextLong(), Long.toOctalString(random.nextLong()), Long.toOctalString(random.nextLong())),
                nullValue());
  }

  @Test
  public void testUsingEhcacheConfiguration() throws Exception {
    // tag::ehcacheBasedConfigurationExample[]
    CacheConfiguration<Long, String> cacheConfiguration = newCacheConfigurationBuilder().buildConfig(Long.class, String.class); //// <1>

    Cache<Long, String> cache = cacheManager.createCache("myCache",
        Eh107Configuration.fromEhcacheCacheConfiguration(cacheConfiguration)); //// <2>

    Eh107Configuration<Long, String> configuration = cache.getConfiguration(Eh107Configuration.class);
    configuration.unwrap(CacheConfiguration.class); //// <3>

    configuration.unwrap(CacheRuntimeConfiguration.class); //// <4>

    try {
      cache.getConfiguration(CompleteConfiguration.class); //// <5>
      fail("IllegalArgumentException expected");
    } catch (IllegalArgumentException iaex) {
      assertThat(iaex.getMessage(), containsString("Cannot unwrap to " + CompleteConfiguration.class));
    }
    // end::ehcacheBasedConfigurationExample[]
  }
}
