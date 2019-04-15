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

package org.ehcache.clustered.replication;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

/**
 * This test asserts Active-Passive fail-over with
 * multi-threaded/multi-client scenarios.
 * Note that fail-over is happening while client threads are still writing
 * Finally the same key set correctness is asserted.
 */
@RunWith(Parameterized.class)
public class BasicClusteredCacheOpsReplicationMultiThreadedTest extends ClusteredTests {

  private static final int NUM_OF_THREADS = 10;
  private static final int JOB_SIZE = 100;
  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">16</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  private static PersistentCacheManager CACHE_MANAGER1;
  private static PersistentCacheManager CACHE_MANAGER2;
  private static Cache<Long, BlobValue> CACHE1;
  private static Cache<Long, BlobValue> CACHE2;

  @Parameters(name = "consistency={0}")
  public static Consistency[] data() {
    return Consistency.values();
  }

  @Parameter
  public Consistency cacheConsistency;

  @ClassRule
  public static Cluster CLUSTER =
      newCluster(2).in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  private final Logger log = LoggerFactory.getLogger(getClass());

  private List<Cache<Long, BlobValue>> caches;

  private final ThreadLocalRandom random = ThreadLocalRandom.current();

  private final ExecutorService executorService = Executors.newWorkStealingPool(NUM_OF_THREADS);

  @Before
  public void startServers() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm-replication"))
            .timeouts(TimeoutsBuilder.timeouts() // we need to give some time for the failover to occur
                .read(Duration.ofMinutes(1))
                .write(Duration.ofMinutes(1)))
            .autoCreate()
            .defaultServerResource("primary-server-resource"));
    CACHE_MANAGER1 = clusteredCacheManagerBuilder.build(true);
    CACHE_MANAGER2 = clusteredCacheManagerBuilder.build(true);
    CacheConfiguration<Long, BlobValue> config = CacheConfigurationBuilder
        .newCacheConfigurationBuilder(Long.class, BlobValue.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder().heap(500, EntryUnit.ENTRIES)
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
        .add(ClusteredStoreConfigurationBuilder.withConsistency(cacheConsistency))
        .build();

    CACHE1 = CACHE_MANAGER1.createCache("clustered-cache", config);
    CACHE2 = CACHE_MANAGER2.createCache("clustered-cache", config);

    caches = Arrays.asList(CACHE1, CACHE2);
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    List<Runnable> unprocessed = executorService.shutdownNow();
    if(!unprocessed.isEmpty()) {
      log.warn("Tearing down with {} unprocess task", unprocessed);
    }
    if(CACHE_MANAGER1 != null && CACHE_MANAGER1.getStatus() != Status.UNINITIALIZED) {
      CACHE_MANAGER1.close();
    }
    if(CACHE_MANAGER2 != null && CACHE_MANAGER2.getStatus() != Status.UNINITIALIZED) {
      CACHE_MANAGER2.close();
      CACHE_MANAGER2.destroy();
    }
  }

  @Test(timeout=180000)
  public void testCRUD() throws Exception {
    Set<Long> universalSet = ConcurrentHashMap.newKeySet();
    List<Future<?>> futures = new ArrayList<>();

    caches.forEach(cache -> {
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        futures.add(executorService.submit(() -> random.longs().limit(JOB_SIZE).forEach(x -> {
          cache.put(x, new BlobValue());
          universalSet.add(x);
        })));
      }
    });

    //This step is to add values in local tier randomly to test invalidations happen correctly
    futures.add(executorService.submit(() -> universalSet.forEach(x -> {
      CACHE1.get(x);
      CACHE2.get(x);
    })));

    CLUSTER.getClusterControl().terminateActive();

    drainTasks(futures);

    Set<Long> readKeysByCache1AfterFailOver = new HashSet<>();
    Set<Long> readKeysByCache2AfterFailOver = new HashSet<>();
    universalSet.forEach(x -> {
      if (CACHE1.get(x) != null) {
        readKeysByCache1AfterFailOver.add(x);
      }
      if (CACHE2.get(x) != null) {
        readKeysByCache2AfterFailOver.add(x);
      }
    });

    assertThat(readKeysByCache2AfterFailOver.size(), equalTo(readKeysByCache1AfterFailOver.size()));

    readKeysByCache2AfterFailOver.stream().forEach(y -> assertThat(readKeysByCache1AfterFailOver.contains(y), is(true)));

  }

  @Test(timeout=180000)
  public void testBulkOps() throws Exception {
    Set<Long> universalSet = ConcurrentHashMap.newKeySet();
    List<Future<?>> futures = new ArrayList<>();

    caches.forEach(cache -> {
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        Map<Long, BlobValue> map = random.longs().limit(JOB_SIZE).collect(HashMap::new, (hashMap, x) -> hashMap.put(x, new BlobValue()), HashMap::putAll);
        futures.add(executorService.submit(() -> {
          cache.putAll(map);
          universalSet.addAll(map.keySet());
        }));
      }
    });

    //This step is to add values in local tier randomly to test invalidations happen correctly
    futures.add(executorService.submit(() -> {
      universalSet.forEach(x -> {
        CACHE1.get(x);
        CACHE2.get(x);
      });
    }));

    CLUSTER.getClusterControl().terminateActive();

    drainTasks(futures);

    Set<Long> readKeysByCache1AfterFailOver = new HashSet<>();
    Set<Long> readKeysByCache2AfterFailOver = new HashSet<>();
    universalSet.forEach(x -> {
      if (CACHE1.get(x) != null) {
        readKeysByCache1AfterFailOver.add(x);
      }
      if (CACHE2.get(x) != null) {
        readKeysByCache2AfterFailOver.add(x);
      }
    });

    assertThat(readKeysByCache2AfterFailOver.size(), equalTo(readKeysByCache1AfterFailOver.size()));

    readKeysByCache2AfterFailOver.stream().forEach(y -> assertThat(readKeysByCache1AfterFailOver.contains(y), is(true)));

  }

  @Ignore("This is currently unstable as if the clear does not complete before the failover," +
          "there is no future operation that will trigger the code in ClusterTierActiveEntity.invokeServerStoreOperation" +
          "dealing with in-flight invalidation reconstructed from reconnect data")
  @Test(timeout=180000)
  public void testClear() throws Exception {
    List<Future<?>> futures = new ArrayList<>();
    Set<Long> universalSet = ConcurrentHashMap.newKeySet();

    caches.forEach(cache -> {
      for (int i = 0; i < NUM_OF_THREADS; i++) {
        Map<Long, BlobValue> map = random.longs().limit(JOB_SIZE).collect(HashMap::new, (hashMap, x) -> hashMap.put(x, new BlobValue()), HashMap::putAll);
        futures.add(executorService.submit(() -> {
          cache.putAll(map);
          universalSet.addAll(map.keySet());
        }));
      }
    });

    drainTasks(futures);

    universalSet.forEach(x -> {
      CACHE1.get(x);
      CACHE2.get(x);
    });

    Future<?> clearFuture = executorService.submit(() -> CACHE1.clear());

    CLUSTER.getClusterControl().terminateActive();

    clearFuture.get();

    universalSet.forEach(x -> assertThat(CACHE2.get(x), nullValue()));

  }

  private void drainTasks(List<Future<?>> futures) throws InterruptedException, java.util.concurrent.ExecutionException {
    for (int i = 0; i < futures.size(); i++) {
      try {
        futures.get(i).get(60, TimeUnit.SECONDS);
      } catch (TimeoutException e) {
        fail("Stuck on number " + i);
      }
    }
  }

  private static class BlobValue implements Serializable {

    private static final long serialVersionUID = 1L;

    private final byte[] data = new byte[10 * 1024];
  }

}
