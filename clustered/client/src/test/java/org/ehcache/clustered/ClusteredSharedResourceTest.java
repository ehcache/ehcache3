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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.net.URI;

import static org.junit.Assert.assertTrue;

public class ClusteredSharedResourceTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");

  private static PersistentCacheManager cacheManager;
  private static Cache<Long, String> sharedCache1;
  private static Cache<String, Long> sharedCache2;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @BeforeClass
  public static void setUp() {
    UnitTestConnectionService.add(CLUSTER_URI,
      new UnitTestConnectionService.PassthroughServerBuilder()
        .resource("primary-server-resource", 8, MemoryUnit.MB)
        .build());

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI).autoCreate()
        .resourcePool("resource-pool-a", 2, MemoryUnit.MB, "primary-server-resource"))
      .build();
    cacheManager.init();

    CacheConfigurationBuilder<Long, String> cacheConfigurationBuilder1 = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a")));

    CacheConfigurationBuilder<String, Long> cacheConfigurationBuilder2 = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, Long.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a")));

    sharedCache1 = cacheManager.createCache("shared-cache-1", cacheConfigurationBuilder1);
    sharedCache2 = cacheManager.createCache("shared-cache-2", cacheConfigurationBuilder2);
  }

  @AfterClass
  public static void tearDown() {
    cacheManager.close();
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testClusteredSharedResource() {
    StringBuilder builder = new StringBuilder();
    for (int k = 0; k < 20; k++) {
      builder.append("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    }
    String value = builder.toString();

    // Fill up the shared resource with elements of sharedCache1
    for (long i = 0L; i < 1_000_000L; i++) {
      sharedCache1.put(i, value);
    }

    // sharedCache should still be able to get its element stored in the shared offheap
    sharedCache2.put("pppppppppppppppp", 1L);
    assertTrue(sharedCache2.containsKey("pppppppppppppppp"));
  }
}

