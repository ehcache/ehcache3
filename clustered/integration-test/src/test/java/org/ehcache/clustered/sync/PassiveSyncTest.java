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

package org.ehcache.clustered.sync;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class PassiveSyncTest {
  private static final String RESOURCE_CONFIG =
    "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
    + "<ohr:offheap-resources>"
    + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">16</ohr:resource>"
    + "</ohr:offheap-resources>" +
    "</service>\n";

  @ClassRule
  public static Cluster CLUSTER =
    new BasicExternalCluster(new File("build/cluster"), 2, Collections.<File>emptyList(), "", RESOURCE_CONFIG, "");

  @Before
  public void startServers() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
  }

  @After
  public void tearDown() throws Exception {
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().startAllServers();
  }

  @Test
  public void testSync() throws Exception {
    CLUSTER.getClusterControl().terminateOnePassive();

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
      = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm"))
        .autoCreate()
        .defaultServerResource("primary-server-resource"));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB))).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

      for (long i = -5; i < 5; i++) {
        cache.put(i, "value" + i);
      }

      CLUSTER.getClusterControl().startOneServer();
      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
      CLUSTER.getClusterControl().terminateActive();
      CLUSTER.getClusterControl().waitForActive();

      for (long i = -5; i < 5; i++) {
        assertThat(cache.get(i), equalTo("value" + i));
      }
    } finally {
      cacheManager.close();
    }
  }

  @Ignore
  @Test
  public void testLifeCycleOperationsOnSync() throws Exception {
    CLUSTER.getClusterControl().terminateOnePassive();

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
      = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm"))
        .autoCreate()
        .defaultServerResource("primary-server-resource"));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true);

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB))).build();

      final Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

      for (long i = 0; i < 100; i++) {
        cache.put(i, "value" + i);
      }

      final CountDownLatch latch = new CountDownLatch(1);
      final AtomicBoolean complete = new AtomicBoolean(false);
      Thread lifeCycleThread = new Thread(new Runnable() {
        @Override
        public void run() {
          while (!complete.get()) {
            try {
              latch.await();
              clusteredCacheManagerBuilder.build(true);
              Thread.sleep(200);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }
        }
      });
      lifeCycleThread.start();
      CLUSTER.getClusterControl().startOneServer();
      latch.countDown();
      CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
      CLUSTER.getClusterControl().terminateActive();
      complete.set(true);

      for (long i = 0; i < 100; i++) {
        assertThat(cache.get(i), equalTo("value" + i));
      }
    } finally {
      cacheManager.close();
    }
  }
}
