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

package org.ehcache.clustered;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.nCopies;
import static java.util.stream.Collectors.toList;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.clustered.client.config.builders.TimeoutsBuilder.timeouts;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.testing.StandardTimeouts.eventually;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

/**
 * Simulate multiple clients starting up the same cache manager simultaneously and ensure that puts and gets works just
 * fine and nothing get lost or hung, just because multiple cache manager instances of the same cache manager are coming up
 * simultaneously.
 */
public class BasicCacheOpsMultiThreadedTest extends ClusteredTests {

  @ClassRule
  public static Cluster CLUSTER =
    newCluster().in(clusterPath()).withServiceFragment(offheapResource("primary-server-resource", 64)).build();

  private static final String CLUSTERED_CACHE_NAME    = "clustered-cache";
  private static final String SYN_CACHE_NAME = "syn-cache";
  private static final String PRIMARY_SERVER_RESOURCE_NAME = "primary-server-resource";
  private static final String CACHE_MANAGER_NAME = "/crud-cm";
  private static final int PRIMARY_SERVER_RESOURCE_SIZE = 4; //MB
  private static final int NUM_THREADS = 8;
  private static final int MAX_WAIT_TIME_SECONDS = 30;

  private final AtomicLong idGenerator = new AtomicLong(2L);

  @Test
  public void testMultipleClients() {
    ExecutorService executorService = Executors.newFixedThreadPool(NUM_THREADS);
    try {
      List<? extends Future<?>> results = nCopies(NUM_THREADS, content()).stream().map(executorService::submit).collect(toList());

      results.stream().map(f -> {
        try {
          f.get();
          return Optional.<Exception>empty();
        } catch (Exception e) {
          return Optional.of(e);
        }
      }).filter(Optional::isPresent).map(Optional::get).reduce((a, b) -> {
          a.addSuppressed(b);
          return a;
        }
      ).ifPresent(t -> {
        throw new AssertionError(t);
      });
    } finally {
      executorService.shutdownNow();
    }
  }

  private Callable<?> content() {
    return () -> {
      try (PersistentCacheManager cacheManager = createCacheManager(CLUSTER.getConnectionURI())) {
        Cache<String, Boolean> synCache = cacheManager.getCache(SYN_CACHE_NAME, String.class, Boolean.class);
        Cache<Long, String> customValueCache = cacheManager.getCache(CLUSTERED_CACHE_NAME, Long.class, String.class);
        parallelPuts(customValueCache);
        String firstClientStartKey = "first_client_start", firstClientEndKey = "first_client_end";
        if (synCache.putIfAbsent(firstClientStartKey, true) == null) {
          customValueCache.put(1L, "value");
          assertThat(customValueCache.get(1L), is("value"));
          synCache.put(firstClientEndKey, true);
        } else {
          assertThat(() -> synCache.get(firstClientEndKey), eventually().matches(notNullValue()));
          assertThat(customValueCache.get(1L), is("value"));
        }
        return null;
      }
    };
  }

  private static PersistentCacheManager createCacheManager(URI clusterURI) {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = newCacheManagerBuilder()
      .with(cluster(clusterURI.resolve(CACHE_MANAGER_NAME))
        .timeouts(timeouts().read(Duration.ofSeconds(MAX_WAIT_TIME_SECONDS)).write(Duration.ofSeconds(MAX_WAIT_TIME_SECONDS)))
        .autoCreate(server -> server.defaultServerResource(PRIMARY_SERVER_RESOURCE_NAME)))
      .withCache(CLUSTERED_CACHE_NAME, newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder()
        .with(clusteredDedicated(PRIMARY_SERVER_RESOURCE_SIZE, MemoryUnit.MB)))
        .withService(new ClusteredStoreConfiguration(Consistency.STRONG)))
      .withCache(SYN_CACHE_NAME, newCacheConfigurationBuilder(String.class, Boolean.class,  newResourcePoolsBuilder()
        .with(clusteredDedicated(PRIMARY_SERVER_RESOURCE_SIZE, MemoryUnit.MB)))
        .withService(new ClusteredStoreConfiguration(Consistency.STRONG)));
    return clusteredCacheManagerBuilder.build(true);
  }

  private void parallelPuts(Cache<Long, String> customValueCache) {
    // make sure each thread gets its own id
    long startingId = idGenerator.getAndAdd(10L);
    customValueCache.put(startingId + 1, "value1");
    customValueCache.put(startingId + 1, "value11");
    customValueCache.put(startingId + 2, "value2");
    customValueCache.put(startingId + 3, "value3");
    customValueCache.put(startingId + 4, "value4");
    assertThat(customValueCache.get(startingId + 1), is("value11"));
    assertThat(customValueCache.get(startingId + 2), is("value2"));
    assertThat(customValueCache.get(startingId + 3), is("value3"));
    assertThat(customValueCache.get(startingId + 4), is("value4"));
  }
}
