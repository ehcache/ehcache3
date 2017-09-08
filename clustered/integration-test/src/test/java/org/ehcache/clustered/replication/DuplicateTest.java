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
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.Ehcache;
import org.ehcache.core.internal.resilience.ResilienceStrategy;
import org.ehcache.core.spi.store.StoreAccessException;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class DuplicateTest {

  private static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
    + "<ohr:offheap-resources>"
    + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">512</ohr:resource>"
    + "</ohr:offheap-resources>" +
    "</config>\n";

  private PersistentCacheManager cacheManager;

  @ClassRule
  public static Cluster CLUSTER =
    newCluster(2).in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @Before
  public void startServers() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  @After
  public void tearDown() throws Exception {
    if(cacheManager != null) {
      cacheManager.close();
      cacheManager.destroy();
    }
  }

  @Test
  public void duplicateAfterFailoverAreReturningTheCorrectResponse() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> builder = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI())
        .autoCreate()
        .defaultServerResource("primary-server-resource"))
      .withCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Integer.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 128, MemoryUnit.MB)))
        .add(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG)));

    cacheManager =  builder.build(true);

    Cache<Integer, String> cache = cacheManager.getCache("cache", Integer.class, String.class);
    switchResilienceStrategy(cache);

    int numEntries = 3000;
    AtomicInteger currentEntry = new AtomicInteger();

    //Perform put operations in another thread
    ExecutorService executorService = Executors.newSingleThreadExecutor();
    try {
      Future<?> puts = executorService.submit((Runnable) () -> {
        while (true) {
          int i = currentEntry.getAndIncrement();
          if (i >= numEntries) {
            break;
          }
          cache.put(i, "value:" + i);
        }
      });

      while (currentEntry.get() < 100); // wait to make sure some entries are added before shutdown

      // Failover to mirror when put & replication are in progress
      CLUSTER.getClusterControl().terminateActive();

      puts.get(30, TimeUnit.SECONDS);

      //Verify cache entries on mirror
      for (int i = 0; i < numEntries; i++) {
        assertThat(cache.get(i)).isEqualTo("value:" + i);
      }
    } finally {
      executorService.shutdownNow();
    }

  }

  private void switchResilienceStrategy(Cache<?,?> cache) throws Exception {
    Field field = Ehcache.class.getDeclaredField("resilienceStrategy");
    field.setAccessible(true);
    ResilienceStrategy<?, ?> newResilienceStrategy = (ResilienceStrategy<?, ?>)
      Proxy.newProxyInstance(cache.getClass().getClassLoader(),
        new Class<?>[] { ResilienceStrategy.class},
        (proxy, method, args) -> {
          System.out.println("In there!!!!!!!!!!!!!!!!!!!!!!!!!");
          fail("Failure on " + method.getName(), findStoreAccessException(args)); // 1 is always the exception
          return null;
        });

    field.set(cache, newResilienceStrategy);
  }

  private StoreAccessException findStoreAccessException(Object[] objects) {
    for(Object o : objects) {
      if (o instanceof StoreAccessException) {
        return (StoreAccessException) o;
      }
    }
    fail("There should be an exception somewhere in " + Arrays.toString(objects));
    return null;
  }
}
