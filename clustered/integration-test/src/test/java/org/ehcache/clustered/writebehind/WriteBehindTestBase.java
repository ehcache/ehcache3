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

package org.ehcache.clustered.writebehind;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.resilience.ThrowingResilienceStrategy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.testing.StandardCluster.offheapResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class WriteBehindTestBase {

  static final String RESOURCE_CONFIG = offheapResource("primary-server-resource", 64);

  static final long KEY = 1L;

  @Rule
  public final TestName testName = new TestName();

  private RecordingLoaderWriter<Long, String> loaderWriter;
  private EvenNumberLoaderWriter<Long, String> evenNumberLoaderWriter;

  @Before
  public void setUp() throws Exception {
    loaderWriter = new RecordingLoaderWriter<>();
  }

  void checkValueFromLoaderWriter(String expected) throws Exception {
    long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(2);

    do {
      try {
        Map<Long, List<String>> records = loaderWriter.getRecords();
        List<String> keyRecords = records.get(KEY);

        assertThat(keyRecords.get(keyRecords.size() - 1), is(expected));
        break;
      } catch (Throwable t) {
        if (System.nanoTime() > deadline) {
          throw (TimeoutException) new TimeoutException("Timeout waiting for writer").initCause(t);
        }
      }
    } while (true);
  }

  void assertValue(Cache<Long, String> cache, String value) {
    assertThat(cache.get(KEY), is(value));
  }

  PersistentCacheManager createCacheManager(URI clusterUri) {
    return getPersistentCacheManager(clusterUri, loaderWriter);
  }

  private PersistentCacheManager getPersistentCacheManager(URI clusterUri, CacheLoaderWriter<Long, String> localLoaderWriter) {
    CacheConfiguration<Long, String> cacheConfiguration =
      newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(10, EntryUnit.ENTRIES)
        .offheap(1, MemoryUnit.MB)
        .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
        .withLoaderWriter(localLoaderWriter)
        .withService(WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration())
        .withResilienceStrategy(new ThrowingResilienceStrategy<>())
        .withService(new ClusteredStoreConfiguration(Consistency.STRONG))
        .build();

    return CacheManagerBuilder
      .newCacheManagerBuilder()
      .with(cluster(clusterUri.resolve("/cm-wb")).timeouts(TimeoutsBuilder.timeouts().read(Duration.ofMinutes(1)).write(Duration.ofMinutes(1))).autoCreate(c -> c))
      .withCache(testName.getMethodName(), cacheConfiguration)
      .build(true);
  }

  PersistentCacheManager createCacheManagerWithLoaderWriterWithFailure(URI clusterUri) {
    evenNumberLoaderWriter = new EvenNumberLoaderWriter<Long, String>();
    return getPersistentCacheManager(clusterUri, evenNumberLoaderWriter);
  }

  protected EvenNumberLoaderWriter<Long, String> getEvenNumberLoaderWriter() {
    return this.evenNumberLoaderWriter;
  }
}
