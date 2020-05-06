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
package org.ehcache.clustered.management;

import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.notification.ContextualNotification;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class CMClosedEventSentTest extends ClusteredTests {

  private static final Map<String, Long> resources;
  static {
    HashMap<String, Long> map = new HashMap<>();
    map.put("primary-server-resource", 64L);
    map.put("secondary-server-resource", 64L);
    resources = unmodifiableMap(map);
  }

  @ClassRule
  public static ClusterWithManagement CLUSTER = new ClusterWithManagement(
    newCluster().in(clusterPath()).withServiceFragment(offheapResources(resources)).build());

  @Test(timeout = 60_000)
  public void test_CACHE_MANAGER_CLOSED() throws Exception {
    try (CacheManager cacheManager = newCacheManagerBuilder().with(cluster(CLUSTER.getCluster().getConnectionURI().resolve("/my-server-entity-1"))
      .autoCreate(server -> server
        .defaultServerResource("primary-server-resource")
        .resourcePool("resource-pool-a", 10, MemoryUnit.MB, "secondary-server-resource") // <2>
        .resourcePool("resource-pool-b", 10, MemoryUnit.MB))) // will take from primary-server-resource
      // management config
      .using(new DefaultManagementRegistryConfiguration()
        .addTags("webapp-1", "server-node-1")
        .setCacheManagerAlias("my-super-cache-manager"))
      // cache config
      .withCache("dedicated-cache-1", newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .with(clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
        .build())
      .build(true)) {

      assertThat(cacheManager.getStatus(), equalTo(Status.AVAILABLE));
      waitFor("CACHE_MANAGER_AVAILABLE");

    }
    waitFor("CACHE_MANAGER_CLOSED");
  }

  private void waitFor(String notifType) throws InterruptedException {
    while (!Thread.currentThread().isInterrupted()) {
      Message message = CLUSTER.getNmsService().waitForMessage();
      if (message.getType().equals("NOTIFICATION")) {
        ContextualNotification notification = message.unwrap(ContextualNotification.class).get(0);
        if (notification.getType().equals(notifType)) {
          break;
        }
      }
    }
    assertFalse(Thread.currentThread().isInterrupted());
  }

}
