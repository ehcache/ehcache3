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

package org.ehcache.clustered.client.docs;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.XmlConfiguration;
import org.junit.After;
import org.junit.Test;

import java.net.URI;

import org.junit.Before;

/**
 * Samples demonstrating use of a clustered cache.
 */
public class GettingStarted {

  @Before
  public void resetPassthroughServer() throws Exception {
    UnitTestConnectionService.add("terracotta://localhost/my-application",
        new UnitTestConnectionService.PassthroughServerBuilder()
            .resource("primary-server-resource", 128, MemoryUnit.MB)
            .resource("secondary-server-resource", 96, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove("terracotta://localhost/my-application");
  }

  @Test
  public void clusteredCacheManagerExample() throws Exception {
    // tag::clusteredCacheManagerExample[]
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder() // <1>
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application")) // <2>
                .autoCreateOnReconnect(c -> c)); // <3>
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true); // <4>

    cacheManager.close(); // <5>
    // end::clusteredCacheManagerExample[]
  }

  @Test
  public void clusteredCacheManagerWithServerSideConfigExample() throws Exception {
    // tag::clusteredCacheManagerWithServerSideConfigExample[]
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
        CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application")).autoCreateOnReconnect(server -> server
                .defaultServerResource("primary-server-resource") // <1>
                .resourcePool("resource-pool-a", 8, MemoryUnit.MB, "secondary-server-resource") // <2>
                .resourcePool("resource-pool-b", 10, MemoryUnit.MB))) // <3>
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, // <4>
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB)))) // <5>
            .withCache("shared-cache-1", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a")))) // <6>
            .withCache("shared-cache-2", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a")))); // <7>
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(true); // <8>

    cacheManager.close();
    // end::clusteredCacheManagerWithServerSideConfigExample[]
  }

  @Test
  public void clusteredCacheManagerWithDynamicallyAddedCacheExample() throws Exception {
    // tag::clusteredCacheManagerWithDynamicallyAddedCacheExample[]
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
                    .autoCreateOnReconnect(server -> server.defaultServerResource("primary-server-resource")
                      .resourcePool("resource-pool-a", 8, MemoryUnit.MB)));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                      .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

    } finally {
      cacheManager.close();
    }
    // end::clusteredCacheManagerWithDynamicallyAddedCacheExample[]
  }

  @Test
  public void explicitConsistencyConfiguration() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
                    .autoCreateOnReconnect(server -> server.defaultServerResource("primary-server-resource")
                      .resourcePool("resource-pool-a", 8, MemoryUnit.MB)));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      // tag::clusteredCacheConsistency[]
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                      .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
          .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG)) // <1>
          .build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      cache.put(42L, "All you need to know!"); // <2>

      // end::clusteredCacheConsistency[]
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void clusteredCacheTieredExample() throws Exception {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
        = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
            .autoCreateOnReconnect(server -> server.defaultServerResource("primary-server-resource")
              .resourcePool("resource-pool-a", 8, MemoryUnit.MB)));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      // tag::clusteredCacheTieredExample[]
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
              .heap(2, MemoryUnit.MB) // <1>
              .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB))) // <2>
          .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG))
          .build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache-tiered", config);
      cache.put(42L, "All you need to know!");

      // end::clusteredCacheTieredExample[]
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void clusteredCacheManagerLifecycleExamples() throws Exception {
    // tag::clusteredCacheManagerLifecycle[]
    CacheManagerBuilder<PersistentCacheManager> autoCreate = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
                .autoCreate(server -> server // <1>
                  .resourcePool("resource-pool", 8, MemoryUnit.MB, "primary-server-resource")))
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool"))));

    CacheManagerBuilder<PersistentCacheManager> autoCreateOnReconnect = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
                .autoCreateOnReconnect(server -> server // <2>
                  .resourcePool("resource-pool", 8, MemoryUnit.MB, "primary-server-resource")))
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool"))));

    CacheManagerBuilder<PersistentCacheManager> expecting = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
                .expecting(server -> server // <3>
                  .resourcePool("resource-pool", 8, MemoryUnit.MB, "primary-server-resource")))
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool"))));

    CacheManagerBuilder<PersistentCacheManager> configless = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application")))
                // <4>
            .withCache("clustered-cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
                ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool"))));
    // end::clusteredCacheManagerLifecycle[]

    autoCreate.build(true).close();
    autoCreateOnReconnect.build(true).close();
    expecting.build(true).close();
    configless.build(true).close();
  }

  @Test
  public void loadDocsXml() throws Exception {
    new XmlConfiguration(getClass().getResource("/configs/docs/ehcache-clustered.xml"));
  }

  @Test
  public void unknownClusteredCacheExample()
  {
    // tag::unspecifiedClusteredCacheExample[]

    CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilderAutoCreate = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
              .autoCreateOnReconnect(server -> server  // <1>
                .resourcePool("resource-pool", 8, MemoryUnit.MB, "primary-server-resource")));

    PersistentCacheManager cacheManager1 = cacheManagerBuilderAutoCreate.build(false);
    cacheManager1.init();
    try {
      CacheConfiguration<Long, String> cacheConfigDedicated = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB)))  // <2>
        .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG))
        .build();

      Cache<Long, String> cacheDedicated = cacheManager1.createCache("my-dedicated-cache", cacheConfigDedicated);  // <3>

      CacheManagerBuilder<PersistentCacheManager> cacheManagerBuilderExpecting = CacheManagerBuilder.newCacheManagerBuilder()
        .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("terracotta://localhost/my-application"))
          .expecting(server -> server  // <4>
            .resourcePool("resource-pool", 8, MemoryUnit.MB, "primary-server-resource")));

      PersistentCacheManager cacheManager2 = cacheManagerBuilderExpecting.build(false);
      cacheManager2.init();
      try {
        CacheConfiguration<Long, String> cacheConfigUnspecified = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
            .with(ClusteredResourcePoolBuilder.clustered()))  // <5>
          .withService(ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG))
          .build();

        Cache<Long, String> cacheUnspecified = cacheManager2.createCache("my-dedicated-cache", cacheConfigUnspecified); // <6>
      } finally {
        cacheManager2.close();
      }
    } finally {
      cacheManager1.close();
    }
    // end::unspecifiedClusteredCacheExample[]
    }

}
