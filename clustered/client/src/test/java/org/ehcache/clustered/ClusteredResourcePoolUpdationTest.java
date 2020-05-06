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

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

public class ClusteredResourcePoolUpdationTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");

  private static PersistentCacheManager cacheManager;
  private static Cache<Long, String> dedicatedCache;
  private static Cache<Long, String> sharedCache;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setUp() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
      new UnitTestConnectionService.PassthroughServerBuilder()
        .resource("primary-server-resource", 8, MemoryUnit.MB)
        .resource("secondary-server-resource", 8, MemoryUnit.MB)
        .build());

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI).autoCreate(server -> server
        .defaultServerResource("primary-server-resource")
        .resourcePool("resource-pool-a", 2, MemoryUnit.MB, "secondary-server-resource")
        .resourcePool("resource-pool-b", 4, MemoryUnit.MB)))
      .withCache("dedicated-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB))))
      .withCache("shared-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a"))))
      .build();
    cacheManager.init();

    dedicatedCache = cacheManager.getCache("dedicated-cache", Long.class, String.class);
    sharedCache = cacheManager.getCache("shared-cache", Long.class, String.class);
  }

  @AfterClass
  public static void tearDown() throws Exception {
    try {
      cacheManager.close();
      UnitTestConnectionService.remove(CLUSTER_URI);
    } finally {
      cacheManager = null;
      dedicatedCache = null;
      sharedCache = null;
    }
  }

  @Test
  public void testClusteredDedicatedResourcePoolUpdation() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Updating CLUSTERED resource is not supported");
    dedicatedCache.getRuntimeConfiguration().updateResourcePools(
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB))
        .build()
    );
  }

  @Test
  public void testClusteredSharedResourcePoolUpdation() throws Exception {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Updating CLUSTERED resource is not supported");
    sharedCache.getRuntimeConfiguration().updateResourcePools(
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a"))
        .build()
    );
  }
}
