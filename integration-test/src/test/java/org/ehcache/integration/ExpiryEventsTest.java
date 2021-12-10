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
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/**
 * Created by alsu on 06/08/15.
 */
public class ExpiryEventsTest {

  private static final ResourcePoolsBuilder resourcePoolsBuilder =
      ResourcePoolsBuilder.newResourcePoolsBuilder().heap(3, EntryUnit.ENTRIES);

  private static final CacheConfigurationBuilder<Long, String> byRefCacheConfigBuilder =
      CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
          .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(1)));

  private static final CacheConfigurationBuilder<Long, String> byValueCacheConfigBuilder =
      byRefCacheConfigBuilder.withService(new DefaultCopierConfiguration<>(
        SerializingCopier.<String>asCopierClass(), DefaultCopierConfiguration.Type.VALUE));

  private static final TestTimeSource testTimeSource = new TestTimeSource();

  private CacheManager cacheManager;

  @Rule
  public TemporaryFolder folder = new TemporaryFolder();

  @Before
  public void setup() throws IOException {
    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(folder.newFolder("tempData")))
        .using(new TimeSourceConfiguration(testTimeSource))
        .build(true);
    testTimeSource.setTimeMillis(0);
  }

  @After
  public void tearDown() {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void testExpiredEventsOnHeapByReference() throws Exception {

    Cache<Long, String> testCache = cacheManager.createCache("onHeapCache",
        byRefCacheConfigBuilder.build());

    performActualTest(testCache);
 }

  @Test
  public void testExpiredEventsOnHeapByValue() throws Exception {

    Cache<Long, String> testCache = cacheManager.createCache("onHeapCache",
        byValueCacheConfigBuilder.build());

    performActualTest(testCache);
  }

  @Test
  public void testExpiredEventsOnHeapAndOffHeapByReference() throws Exception {

    CacheConfigurationBuilder<Long, String> configBuilder = byRefCacheConfigBuilder.withResourcePools(
        resourcePoolsBuilder.offheap(1, MemoryUnit.MB));
    Cache<Long, String> testCache = cacheManager.createCache("onHeapOffHeapCache",
        configBuilder.build());

    performActualTest(testCache);
  }

  @Test
  public void testExpiredEventsOnHeapAndOffHeapByValue() throws Exception {

    CacheConfigurationBuilder<Long, String> configBuilder = byValueCacheConfigBuilder.withResourcePools(
        resourcePoolsBuilder.offheap(1, MemoryUnit.MB));
    Cache<Long, String> testCache = cacheManager.createCache("onHeapOffHeapCache",
        configBuilder.build());

    performActualTest(testCache);
  }

  @Test
  public void testExpiredEventsOnHeapAndDiskByReference() throws Exception {

    CacheConfigurationBuilder<Long, String> configBuilder = byRefCacheConfigBuilder.withResourcePools(
        resourcePoolsBuilder.disk(1, MemoryUnit.MB));
    Cache<Long, String> testCache = cacheManager.createCache("onHeapDiskCache",
        configBuilder.build());

    performActualTest(testCache);
  }

  @Test
  public void testExpiredEventsOnHeapAndDiskByValue() throws Exception {

    CacheConfigurationBuilder<Long, String> configBuilder = byValueCacheConfigBuilder.withResourcePools(
        resourcePoolsBuilder.disk(1, MemoryUnit.MB));
    Cache<Long, String> testCache = cacheManager.createCache("onHeapDiskCache",
        configBuilder.build());

    performActualTest(testCache);
  }

  @Test
  public void testExpiredEventsOnHeapAndOffHeapAndDiskByReference() throws Exception {

    CacheConfigurationBuilder<Long, String> configBuilder = byRefCacheConfigBuilder.withResourcePools(
        resourcePoolsBuilder.offheap(1, MemoryUnit.MB).disk(2, MemoryUnit.MB));
    Cache<Long, String> testCache = cacheManager.createCache("onHeapOffHeapDiskCache",
        configBuilder.build());

    performActualTest(testCache);
  }

  @Test
  public void testExpiredEventsOnHeapAndOffHeapAndDiskByValue() throws Exception {

    CacheConfigurationBuilder<Long, String> configBuilder = byValueCacheConfigBuilder.withResourcePools(
        resourcePoolsBuilder.offheap(1, MemoryUnit.MB).disk(2, MemoryUnit.MB));
    Cache<Long, String> testCache = cacheManager.createCache("onHeapOffHeapDiskCache",
        configBuilder.build());

    performActualTest(testCache);
  }

  private void performActualTest(Cache<Long, String> testCache) {

    final List<Long> expiredKeys = new CopyOnWriteArrayList<>();

    testCache.getRuntimeConfiguration().registerCacheEventListener(event -> expiredKeys.add(event.getKey()), EventOrdering.ORDERED, EventFiring.SYNCHRONOUS, EnumSet.of(EventType.EXPIRED));

    testCache.put(1L, "one");
    testCache.put(2L, "two");
    testCache.put(3L, "three");
    testCache.put(4L, "four");
    testCache.put(5L, "five");
    testCache.put(6L, "six");
    testCache.put(7L, "seven");

    testCache.get(1L);
    testCache.get(2L);
    testCache.get(3L);
    testCache.get(4L);
    testCache.get(5L);

    testTimeSource.setTimeMillis(1100);

    testCache.get(1L);
    testCache.get(2L);
    testCache.get(3L);
    testCache.get(4L);
    testCache.get(5L);
    testCache.get(6L);
    testCache.get(7L);

    assertThat(expiredKeys, containsInAnyOrder(1L, 2L, 3L, 4L, 5L, 6L, 7L));
  }

}
