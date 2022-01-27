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

package org.ehcache.jsr107;

import java.lang.management.ManagementFactory;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.jsr107.config.ConfigurationElementState;
import org.ehcache.jsr107.config.Jsr107CacheConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/**
 * ConfigStatsManagementActivationTest
 */
public class ConfigStatsManagementActivationTest {

  private static final String MBEAN_MANAGEMENT_TYPE = "CacheConfiguration";
  private static final String MBEAN_STATISTICS_TYPE = "CacheStatistics";

  private MBeanServer server = ManagementFactory.getPlatformMBeanServer();

  private CachingProvider provider;

  @Before
  public void setUp() {
    provider = Caching.getCachingProvider();
  }

  @After
  public void tearDown() {
    provider.close();
  }

  @Test
  public void testEnabledAtCacheLevel() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-mbeans-cache-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("stringCache", String.class, String.class);
    @SuppressWarnings("unchecked")
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(true));

    assertThat(isMbeanRegistered("stringCache", MBEAN_MANAGEMENT_TYPE), is(true));
    assertThat(isMbeanRegistered("stringCache", MBEAN_STATISTICS_TYPE), is(true));
  }

  @Test
  public void testEnabledAtCacheManagerLevel() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/org/ehcache/docs/ehcache-107-mbeans-cache-manager-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("stringCache", String.class, String.class);
    @SuppressWarnings("unchecked")
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(true));

    assertThat(isMbeanRegistered("stringCache", MBEAN_MANAGEMENT_TYPE), is(true));
    assertThat(isMbeanRegistered("stringCache", MBEAN_STATISTICS_TYPE), is(true));
  }

  @Test
  public void testCacheLevelOverridesCacheManagerLevel() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/org/ehcache/docs/ehcache-107-mbeans-cache-manager-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("overrideCache", String.class, String.class);
    @SuppressWarnings("unchecked")
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(false));
    assertThat(configuration.isStatisticsEnabled(), is(false));

    assertThat(isMbeanRegistered("overrideCache", MBEAN_MANAGEMENT_TYPE), is(false));
    assertThat(isMbeanRegistered("overrideCache", MBEAN_STATISTICS_TYPE), is(false));
  }

  @Test
  public void testCacheLevelOnlyOneOverridesCacheManagerLevel() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/org/ehcache/docs/ehcache-107-mbeans-cache-manager-config.xml")
        .toURI(), provider.getDefaultClassLoader());

    Cache<String, String> cache = cacheManager.getCache("overrideOneCache", String.class, String.class);
    @SuppressWarnings("unchecked")
    Eh107Configuration<String, String> configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(false));

    assertThat(isMbeanRegistered("overrideOneCache", MBEAN_MANAGEMENT_TYPE), is(true));
    assertThat(isMbeanRegistered("overrideOneCache", MBEAN_STATISTICS_TYPE), is(false));
  }

  @Test
  public void testEnableCacheLevelProgrammatic() throws Exception {
    CacheManager cacheManager = provider.getCacheManager();

    CacheConfigurationBuilder<Long, String> configurationBuilder = newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .withService(new Jsr107CacheConfiguration(ConfigurationElementState.ENABLED, ConfigurationElementState.ENABLED));
    Cache<Long, String> cache = cacheManager.createCache("test", Eh107Configuration.fromEhcacheCacheConfiguration(configurationBuilder));

    @SuppressWarnings("unchecked")
    Eh107Configuration<Long, String> configuration = cache.getConfiguration(Eh107Configuration.class);
    assertThat(configuration.isManagementEnabled(), is(true));
    assertThat(configuration.isStatisticsEnabled(), is(true));

    assertThat(isMbeanRegistered("test", MBEAN_MANAGEMENT_TYPE), is(true));
    assertThat(isMbeanRegistered("test", MBEAN_STATISTICS_TYPE), is(true));
  }

  private boolean isMbeanRegistered(String cacheName, String type) throws MalformedObjectNameException {
    String query = "javax.cache:type=" + type + ",CacheManager=*,Cache=" + cacheName;
    return server.queryMBeans(ObjectName.getInstance(query), null).size() == 1;
  }

  @Test
  public void testManagementDisabledOverriddenFromTemplate() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-mbeans-template-config.xml")
            .toURI(),
        provider.getDefaultClassLoader());

    MutableConfiguration<Long, String> configuration = new MutableConfiguration<>();
    configuration.setTypes(Long.class, String.class);
    configuration.setManagementEnabled(false);
    configuration.setStatisticsEnabled(false);

    Cache<Long, String> cache = cacheManager.createCache("enables-mbeans", configuration);

    @SuppressWarnings("unchecked")
    Eh107Configuration<Long, String> eh107Configuration = cache.getConfiguration(Eh107Configuration.class);
    assertThat(eh107Configuration.isManagementEnabled(), is(true));
    assertThat(eh107Configuration.isStatisticsEnabled(), is(true));

    assertThat(isMbeanRegistered("enables-mbeans", MBEAN_MANAGEMENT_TYPE), is(true));
    assertThat(isMbeanRegistered("enables-mbeans", MBEAN_STATISTICS_TYPE), is(true));
  }

  @Test
  public void testManagementEnabledOverriddenFromTemplate() throws Exception {
    CacheManager cacheManager = provider.getCacheManager(getClass().getResource("/ehcache-107-mbeans-template-config.xml")
            .toURI(),
        provider.getDefaultClassLoader());

    MutableConfiguration<Long, String> configuration = new MutableConfiguration<>();
    configuration.setTypes(Long.class, String.class);
    configuration.setManagementEnabled(true);
    configuration.setStatisticsEnabled(true);

    Cache<Long, String> cache = cacheManager.createCache("disables-mbeans", configuration);

    @SuppressWarnings("unchecked")
    Eh107Configuration<Long, String> eh107Configuration = cache.getConfiguration(Eh107Configuration.class);
    assertThat(eh107Configuration.isManagementEnabled(), is(false));
    assertThat(eh107Configuration.isStatisticsEnabled(), is(false));

    assertThat(isMbeanRegistered("disables-mbeans", MBEAN_MANAGEMENT_TYPE), is(false));
    assertThat(isMbeanRegistered("disables-mbeans", MBEAN_STATISTICS_TYPE), is(false));
  }

  @Test
  public void basicJsr107StillWorks() throws Exception {
    CacheManager cacheManager = provider.getCacheManager();

    MutableConfiguration<Long, String> configuration = new MutableConfiguration<>();
    configuration.setTypes(Long.class, String.class);
    configuration.setManagementEnabled(true);
    configuration.setStatisticsEnabled(true);

    Cache<Long, String> cache = cacheManager.createCache("cache", configuration);
    @SuppressWarnings("unchecked")
    Eh107Configuration<Long, String> eh107Configuration = cache.getConfiguration(Eh107Configuration.class);

    assertThat(eh107Configuration.isManagementEnabled(), is(true));
    assertThat(eh107Configuration.isStatisticsEnabled(), is(true));

    assertThat(isMbeanRegistered("cache", MBEAN_MANAGEMENT_TYPE), is(true));
    assertThat(isMbeanRegistered("cache", MBEAN_STATISTICS_TYPE), is(true));
  }
}
