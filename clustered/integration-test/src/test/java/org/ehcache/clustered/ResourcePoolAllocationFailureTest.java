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

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.internal.exceptions.InvalidServerStoreConfigurationException;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ResourcePoolAllocationFailureTest {

  private static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER =
    new BasicExternalCluster(new File("build/cluster"), 1, Collections.<File>emptyList(), "", RESOURCE_CONFIG, "");

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Test
  public void testTooLowResourceException() throws InterruptedException {

    DedicatedClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(10, MemoryUnit.KB);
    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder = getPersistentCacheManagerCacheManagerBuilder(resourcePool);

    try {
      cacheManagerBuilder.build(true);
      fail("InvalidServerStoreConfigurationException expected");
    } catch (Exception e) {
      e.printStackTrace();
      assertThat(getRootCause(e), instanceOf(InvalidServerStoreConfigurationException.class));
      assertThat(getRootCause(e).getMessage(), startsWith("Failed to create ServerStore"));
    }
    resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(100, MemoryUnit.KB);
    cacheManagerBuilder = getPersistentCacheManagerCacheManagerBuilder(resourcePool);
    PersistentCacheManager persistentCacheManager = cacheManagerBuilder.build(true);

    assertThat(persistentCacheManager, notNullValue());
    persistentCacheManager.close();

  }

  private CacheManagerBuilder<PersistentCacheManager> getPersistentCacheManagerCacheManagerBuilder(DedicatedClusteredResourcePool resourcePool) {

    ClusteringServiceConfigurationBuilder clusteringServiceConfigurationBuilder = ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm"));
    ServerSideConfigurationBuilder serverSideConfigurationBuilder = clusteringServiceConfigurationBuilder.autoCreate()
      .defaultServerResource("primary-server-resource");

    return CacheManagerBuilder.newCacheManagerBuilder()
      .with(serverSideConfigurationBuilder)
      .withCache("test-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(resourcePool)
      ).add(new ClusteredStoreConfiguration(Consistency.EVENTUAL)));
  }

  private static Throwable getRootCause(Throwable e) {
    Throwable current = e;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }


}
