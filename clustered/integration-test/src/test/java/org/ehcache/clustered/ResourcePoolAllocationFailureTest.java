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

import org.ehcache.clustered.client.internal.PerpetualCachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class ResourcePoolAllocationFailureTest extends ClusteredTests {

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(clusterPath())
    .withServiceFragment(offheapResource("primary-server-resource", 64)).build();

  @Test
  public void testTooLowResourceException() throws InterruptedException {

    DedicatedClusteredResourcePool resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(10, MemoryUnit.KB);
    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilder = getPersistentCacheManagerCacheManagerBuilder(resourcePool);

    try {
      cacheManagerBuilder.build(true);
      fail("InvalidServerStoreConfigurationException expected");
    } catch (Exception e) {
      Throwable cause = getCause(e, PerpetualCachePersistenceException.class);
      assertThat(cause, notNullValue());
      assertThat(cause.getMessage(), startsWith("Unable to create"));
    }
    resourcePool = ClusteredResourcePoolBuilder.clusteredDedicated(100, MemoryUnit.KB);
    cacheManagerBuilder = getPersistentCacheManagerCacheManagerBuilder(resourcePool);
    PersistentCacheManager persistentCacheManager = cacheManagerBuilder.build(true);

    assertThat(persistentCacheManager, notNullValue());
    persistentCacheManager.close();

  }

  private CacheManagerBuilder<PersistentCacheManager> getPersistentCacheManagerCacheManagerBuilder(DedicatedClusteredResourcePool resourcePool) {

    ClusteringServiceConfigurationBuilder clusteringServiceConfigurationBuilder = ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/crud-cm"));
    ClusteringServiceConfiguration clusteringConfiguration = clusteringServiceConfigurationBuilder.autoCreate(server -> server.defaultServerResource("primary-server-resource")).build();

    return CacheManagerBuilder.newCacheManagerBuilder()
      .with(clusteringConfiguration)
      .withCache("test-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(resourcePool)
      ).withService(new ClusteredStoreConfiguration(Consistency.EVENTUAL)));
  }

  private static Throwable getCause(Throwable e, Class<? extends Throwable> causeClass) {
    Throwable current = e;
    while (current.getCause() != null) {
      if (current.getClass().isAssignableFrom(causeClass)) {
        return current;
      }
      current = current.getCause();
    }
    return null;
  }

  private static Throwable getRootCause(Throwable e) {
    Throwable current = e;
    while (current.getCause() != null) {
      current = current.getCause();
    }
    return current;
  }


}
