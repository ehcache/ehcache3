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

import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class DestroyLoopTest extends ClusteredTests {

  private static final String CACHE_MANAGER_NAME = "/destroy-cm";
  private static final String CACHE_NAME = "clustered-cache";

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(clusterPath())
    .withServiceFragment(offheapResource("primary-server-resource", 64)).build();

  @Test
  public void testDestroyLoop() throws Exception {
    for (int i = 0; i < 10; i++) {
      try (CacheManagerContainer cmc = new CacheManagerContainer(10, this::createCacheManager)) {
        // just put in one and get from another
        cmc.cacheManagerList.get(0).getCache(CACHE_NAME, Long.class, String.class).put(1L, "value");
        assertThat(cmc.cacheManagerList.get(5).getCache(CACHE_NAME, Long.class, String.class).get(1L),
          is("value"));
      }
      destroyCacheManager();
    }
  }

  private void destroyCacheManager() throws CachePersistenceException {
    PersistentCacheManager cacheManager = newCacheManagerBuilder().with(
      ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve(CACHE_MANAGER_NAME))
        .expecting(c -> c)).build(false);
    cacheManager.destroy();
  }

  private PersistentCacheManager createCacheManager() {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
      newCacheManagerBuilder()
        .with(cluster(CLUSTER.getConnectionURI().resolve(CACHE_MANAGER_NAME)).autoCreate(c -> c))
        .withCache(CACHE_NAME, newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES)
            .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
          .withService(new ClusteredStoreConfiguration(Consistency.STRONG)));
    return clusteredCacheManagerBuilder.build(true);
  }

  private static class CacheManagerContainer implements AutoCloseable {
    private final List<PersistentCacheManager> cacheManagerList;

    private CacheManagerContainer(int numCacheManagers, Supplier<PersistentCacheManager> cmSupplier) {
      List<PersistentCacheManager> cm = new ArrayList<>();
      for (int i = 0; i < numCacheManagers; i++) {
        cm.add(cmSupplier.get());
      }
      cacheManagerList = Collections.unmodifiableList(cm);
    }

    @Override
    public void close() throws StateTransitionException {
      cacheManagerList.forEach(PersistentCacheManager::close);
    }
  }
}
