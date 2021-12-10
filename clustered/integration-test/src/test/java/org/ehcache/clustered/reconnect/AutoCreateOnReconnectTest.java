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
package org.ehcache.clustered.reconnect;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.config.units.MemoryUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.testing.StandardTimeouts.eventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class AutoCreateOnReconnectTest extends ClusteredTests {

  @ClassRule
  public static Cluster CLUSTER = newCluster(1).in(clusterPath())
    .withServiceFragment(offheapResource("primary-server-resource", 64)).build();

  @Test
  public void cacheManagerCanReconnect() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();

    try (PersistentCacheManager cacheManager = newCacheManagerBuilder()
      .with(cluster(connectionURI.resolve("/crud-cm"))
        .autoCreateOnReconnect(server -> server.defaultServerResource("primary-server-resource")))
      .build(true)) {

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache",
        newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder()
          .with(clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
          .build());

      cache.put(1L, "one");

      CLUSTER.getClusterControl().terminateAllServers();
      CLUSTER.getClusterControl().startAllServers();

      assertThat(() -> {
        cache.put(1L, "two");
        return cache.get(1L);
      }, eventually().is("two"));
    }
  }
}
