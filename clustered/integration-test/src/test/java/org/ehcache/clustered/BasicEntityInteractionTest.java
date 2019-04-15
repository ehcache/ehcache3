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

import java.io.File;
import java.net.URI;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.common.EhcacheEntityVersion;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.internal.statistics.DefaultStatisticsService;
import org.ehcache.management.cluster.DefaultClusteringManagementService;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.testing.rules.Cluster;

import static java.util.Collections.emptyMap;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.config.units.EntryUnit.ENTRIES;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class BasicEntityInteractionTest extends ClusteredTests {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">4</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();
  private ClusterTierManagerConfiguration blankConfiguration = new ClusterTierManagerConfiguration("identifier", new ServerSideConfiguration(emptyMap()));

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Rule
  public TestName testName= new TestName();

  @Test
  public void testClusteringServiceConfigurationBuilderThrowsNPE() throws Exception {
    String cacheName = "myCACHE";
    String offheap = "primary-server-resource";
    URI tsaUri = CLUSTER.getConnectionURI();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache(cacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(100, ENTRIES)
        .with(clusteredDedicated(offheap, 2, MemoryUnit.MB)))
      ).with(ClusteringServiceConfigurationBuilder.cluster(tsaUri)
        .autoCreate()
        .defaultServerResource(offheap)
      ).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(cacheName, Long.class, String.class);
      cache.put(1L, "one");
    }

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache(cacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
          .heap(100, ENTRIES)
          .with(clusteredDedicated(offheap, 2, MemoryUnit.MB))
        )
      ).with(ClusteringServiceConfigurationBuilder.cluster(tsaUri)
        // these two shouldn't be needed as the clustered cache entity has already been created
//              .autoCreate()
//              .defaultServerResource(offheap)
      ).using(new DefaultStatisticsService()
      ).using(new DefaultClusteringManagementService()
      ).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(cacheName, Long.class, String.class);
      cache.get(1L);
    }

  }

  @Test
  public void testServicesStoppedTwice() throws Exception {
    String cacheName = "myCACHE";
    String offheap = "primary-server-resource";
    URI tsaUri = CLUSTER.getConnectionURI();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache(cacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(100, ENTRIES)
        .with(clusteredDedicated(offheap, 2, MemoryUnit.MB)))
      ).with(ClusteringServiceConfigurationBuilder.cluster(tsaUri)
        .autoCreate()
        .defaultServerResource(offheap)
        // manually adding the following two services should work
      ).using(new DefaultStatisticsService()
      ).using(new DefaultClusteringManagementService()
      ).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(cacheName, Long.class, String.class);
      cache.put(1L, "one");
    }

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .withCache(cacheName, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
          .heap(100, ENTRIES)
          .with(clusteredDedicated(offheap, 2, MemoryUnit.MB))
        )
      ).with(ClusteringServiceConfigurationBuilder.cluster(tsaUri)
      ).build(true)) {
      Cache<Long, String> cache = cacheManager.getCache(cacheName, Long.class, String.class);
      cache.get(1L);
    }

  }

  @Test
  public void testAbsentEntityRetrievalFails() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      try {
        ref.fetchEntity(null);
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    }
  }

  @Test
  public void testAbsentEntityCreationSucceeds() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      assertThat(ref.fetchEntity(null), not(Matchers.nullValue()));
    }
  }

  @Test
  public void testPresentEntityCreationFails() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      try {
        try {
          ref.create(blankConfiguration);
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }

        ClusterTierManagerConfiguration otherConfiguration = new ClusterTierManagerConfiguration("different", new ServerSideConfiguration(emptyMap()));
        try {
          ref.create(otherConfiguration);
          fail("Expected EntityAlreadyExistsException");
        } catch (EntityAlreadyExistsException e) {
          //expected
        }
      } finally {
        ref.destroy();
      }
    }
  }

  @Test
  public void testAbsentEntityDestroyFails() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      try {
        ref.destroy();
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    }
  }

  @Test
  public void testPresentEntityDestroySucceeds() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.destroy();

      try {
        ref.fetchEntity(null);
        fail("Expected EntityNotFoundException");
      } catch (EntityNotFoundException e) {
        //expected
      }
    }
  }

  @Test
  @Ignore
  @SuppressWarnings("try")
  public void testPresentEntityDestroyBlockedByHeldReferenceSucceeds() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);

      try (ClusterTierManagerClientEntity entity = ref.fetchEntity(null)) {
        ref.destroy();
      }
    }
  }

  @Test
  public void testPresentEntityDestroyNotBlockedByReleasedReferenceSucceeds() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.fetchEntity(null).close();
      ref.destroy();
    }
  }

  @Test
  public void testDestroyedEntityAllowsRecreation() throws Throwable {
    try (Connection client = CLUSTER.newConnection()) {
      EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);

      ref.create(blankConfiguration);
      ref.destroy();

      ref.create(blankConfiguration);
      assertThat(ref.fetchEntity(null), not(nullValue()));
    }
  }

  private EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> getEntityRef(Connection client) throws org.terracotta.exception.EntityNotProvidedException {
    return client.getEntityRef(ClusterTierManagerClientEntity.class, EhcacheEntityVersion.ENTITY_VERSION, testName.getMethodName());
  }
}
