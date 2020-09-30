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

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.util.ParallelTestCluster;
import org.ehcache.clustered.util.runners.Parallel;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

@RunWith(Parallel.class)
public class BasicLifeCyclePassiveReplicationTest extends ClusteredTests {

  @ClassRule @Rule
  public static final ParallelTestCluster CLUSTER = new ParallelTestCluster(newCluster(2).in(clusterPath())
    .withServerHeap(512)
    .withServiceFragment(offheapResource("primary-server-resource", 16)).build());

  @Before
  public void startServers() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
  }

  @Test
  public void testDestroyCacheManager() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> configBuilder = newCacheManagerBuilder().with(cluster(CLUSTER.getConnectionURI().resolve("/destroy-CM"))
      .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    PersistentCacheManager cacheManager1 = configBuilder.build(true);
    PersistentCacheManager cacheManager2 = configBuilder.build(true);

    cacheManager2.close();

    try {
      cacheManager2.destroy();
      fail("Exception expected");
    } catch (Exception e) {
      e.printStackTrace();
    }

    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    CLUSTER.getClusterControl().terminateActive();

    cacheManager1.createCache("test", newCacheConfigurationBuilder(Long.class, String.class, heap(10).with(clusteredDedicated(10, MB))));
  }

  @Test
  public void testDestroyLockEntity() throws Exception {
    VoltronReadWriteLock lock1 = new VoltronReadWriteLock(CLUSTER.newConnection(), "my-lock");
    VoltronReadWriteLock.Hold hold1 = lock1.tryReadLock();

    VoltronReadWriteLock lock2 = new VoltronReadWriteLock(CLUSTER.newConnection(), "my-lock");
    assertThat(lock2.tryWriteLock(), nullValue());

    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    CLUSTER.getClusterControl().terminateActive();

    hold1.unlock();
  }

}
