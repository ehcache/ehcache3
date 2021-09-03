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
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.EhcacheManager;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.integration.statistics.AbstractCacheCalculationTest;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.Service;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNullPointerException;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * @author Henri Tremblay
 */
@RunWith(Parameterized.class)
public class EhcacheBaseTest extends AbstractCacheCalculationTest {

  private CacheManager cacheManager;

  private Cache<Integer, String> cache;

  private final StatisticsService statisticsService = new DefaultStatisticsService();

  private final TestTimeSource timeSource = new TestTimeSource();

  public EhcacheBaseTest(ResourcePoolsBuilder poolBuilder) {
    super(poolBuilder);
  }

  @After
  public void after() {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  private void createCacheManager(CacheManagerBuilder<CacheManager> builder) {
    cacheManager = builder
      .build(true);
  }

  private void createNotAtomicCacheManager() throws IOException {
    Configuration config = ConfigurationBuilder.newConfigurationBuilder()
      .withService(new TimeSourceConfiguration(timeSource))
      .withService(new DefaultPersistenceConfiguration(diskPath.newFolder()))
      .build();

    Collection<Service> services = Collections.singleton(statisticsService);
    cacheManager = new EhcacheManager(config, services, false);
    cacheManager.init();
  }

  private void createCacheManager() {
    createCacheManager(baseCacheManagerConfig());
  }

  private CacheManagerBuilder<CacheManager> baseCacheManagerConfig() {
    try {
      return CacheManagerBuilder.newCacheManagerBuilder()
        .using(new DefaultPersistenceConfiguration(diskPath.newFolder()))
        .using(statisticsService)
        .using(new TimeSourceConfiguration(timeSource));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Cache<Integer, String> createCache() {
    return createCache(baseConfig());
  }

  private Cache<Integer, String> createCache(CacheConfigurationBuilder<Integer, String> config) {
    Cache<Integer, String> cache = cacheManager.createCache("cache", config);
    cacheStatistics = statisticsService.getCacheStatistics("cache");
    return cache;
  }

  private CacheConfigurationBuilder<Integer, String> baseConfig() {
    return CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class, resources);
  }

  @Test
  public void putIfAbsent_absent() {
    createCacheManager();

    cache = createCache();

    assertThat(cache.putIfAbsent(1, "a")).isNull();

    assertThat(cache.get(1)).isEqualTo("a");

    changesOf(1, 1, 1, 0);
  }

  @Test
  public void putIfAbsent_present() {
    createCacheManager();

    cache = createCache();

    cache.put(1, "a");

    assertThat(cache.putIfAbsent(1, "b")).isEqualTo("a");

    changesOf(1, 0, 1, 0);
  }

  @Test
  public void putIfAbsent_presentButExpired() {
    createCacheManager();

    CacheConfigurationBuilder<Integer, String> builder = baseConfig()
      .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10)));
    cache = createCache(builder);

    cache.put(1, "a");

    timeSource.advanceTime(15);

    assertThat(cache.putIfAbsent(1, "b")).isNull();

    assertThat(cache.get(1)).isEqualTo("b");

    changesOf(1, 1, 2, 0);
  }

  @Test
  public void putIfAbsent_absentPutNull() {
    createCacheManager();

    cache = createCache();

    assertThatNullPointerException().isThrownBy(() -> cache.putIfAbsent(1, null));

    changesOf(0, 0, 0, 0);
  }

  @Test
  public void putIfAbsentLoaderWriter_absentAndLoaded() throws Exception {
    createCacheManager();

    CacheLoaderWriter<Integer, String> loader = mockLoader();
    when(loader.load(1)).thenReturn("a");

    CacheConfigurationBuilder<Integer, String> builder = baseConfig()
      .withLoaderWriter(loader);
    cache = createCache(builder);

    assertThat(cache.putIfAbsent(1, "b")).isEqualTo("a");

    assertThat(cache.get(1)).isEqualTo("a");

    changesOf(2, 0, 0, 0);
  }

  @Test
  public void putIfAbsentLoaderWriter_absentAndNotLoaded() throws Exception {
    createCacheManager();

    CacheLoaderWriter<Integer, String> loader = mockLoader();
    when(loader.load(1)).thenReturn(null);

    CacheConfigurationBuilder<Integer, String> builder = baseConfig()
      .withLoaderWriter(loader);
    cache = createCache(builder);

    assertThat(cache.putIfAbsent(1, "b")).isNull();

    verify(loader).write(1, "b");

    assertThat(cache.get(1)).isEqualTo("b");

    changesOf(1, 1, 1, 0);
  }

  @Test
  public void putIfAbsentLoaderWriter_present() throws Exception {
    createCacheManager();

    CacheLoaderWriter<Integer, String> loader = mockLoader();

    CacheConfigurationBuilder<Integer, String> builder = baseConfig()
      .withLoaderWriter(loader);
    cache = createCache(builder);

    cache.put(1, "a");

    assertThat(cache.putIfAbsent(1, "b")).isEqualTo("a");

    verify(loader).write(1, "a");

    changesOf(1, 0, 1, 0);
  }

  @Test
  public void putIfAbsentLoaderWriter_presentButExpiredAndLoaded() throws Exception {
    createCacheManager();

    CacheLoaderWriter<Integer, String> loader = mockLoader();
    when(loader.load(1)).thenReturn("c");

    CacheConfigurationBuilder<Integer, String> builder = baseConfig()
      .withLoaderWriter(loader)
      .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10)));
    cache = createCache(builder);

    cache.put(1, "a");

    timeSource.advanceTime(15);

    assertThat(cache.putIfAbsent(1, "b")).isEqualTo("c");

    verify(loader).write(1, "a");

    changesOf(1, 0, 1, 0);
  }

  @Test
  public void putIfAbsentLoaderWriter_presentButExpiredAndNotLoaded() throws Exception {
    createCacheManager();

    CacheLoaderWriter<Integer, String> loader = mockLoader();
    when(loader.load(1)).thenReturn(null);

    CacheConfigurationBuilder<Integer, String> builder = baseConfig()
      .withLoaderWriter(loader)
      .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10)));
    cache = createCache(builder);

    cache.put(1, "a");

    timeSource.advanceTime(15);

    assertThat(cache.putIfAbsent(1, "b")).isNull();

    verify(loader).write(1, "b");

    changesOf(0, 1, 2, 0);
  }

  @Test
  public void putIfAbsentLoaderWriterNotAtomic_absent() throws Exception {
    createNotAtomicCacheManager();

    CacheLoaderWriter<Integer, String> loader = mockLoader();

    CacheConfigurationBuilder<Integer, String> builder = baseConfig()
      .withLoaderWriter(loader);
    cache = createCache(builder);

    assertThat(cache.putIfAbsent(1, "a")).isNull();

    verify(loader).write(1, "a");

    assertThat(cache.get(1)).isEqualTo("a");

    changesOf(1, 1, 1, 0);
  }

  @Test
  public void putIfAbsentLoaderWriterNotAtomic_present() throws Exception {
    createNotAtomicCacheManager();

    CacheLoaderWriter<Integer, String> loader = mockLoader();

    CacheConfigurationBuilder<Integer, String> builder = baseConfig()
      .withLoaderWriter(loader);
    cache = createCache(builder);

    cache.put(1, "a");

    assertThat(cache.putIfAbsent(1, "b")).isEqualTo("a");

    verify(loader).write(1, "a");
    verifyNoMoreInteractions(loader);

    changesOf(1, 0, 1, 0);
  }

  @SuppressWarnings("unchecked")
  private static CacheLoaderWriter<Integer, String> mockLoader() {
    return mock(CacheLoaderWriter.class);
  }
}
