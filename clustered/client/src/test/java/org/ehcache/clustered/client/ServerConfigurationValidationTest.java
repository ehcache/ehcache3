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

package org.ehcache.clustered.client;

import java.net.URI;
import org.ehcache.CachePersistenceException;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import static org.junit.Assert.fail;
import org.junit.Before;
import org.junit.Test;

public class ServerConfigurationValidationTest {

  private static final URI CLUSTER_URI = URI.create("http://example.com:9540/cache-manager");

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
        new UnitTestConnectionService.PassthroughServerBuilder()
            .resource("primary-server-resource", 64, MemoryUnit.MB)
            .resource("secondary-server-resource", 64, MemoryUnit.MB)
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testIdenticalConfiguration() throws CachePersistenceException {
    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .autoCreate(true)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));

      clusteredCacheManagerBuilder.build(true).close();
    }

    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));
      PersistentCacheManager cm = clusteredCacheManagerBuilder.build(true);

      cm.close();
      cm.destroy();
    }
  }

  @Test
  public void testIdenticalPoolsWithAbsentDefaultConfiguration() throws CachePersistenceException {
    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .autoCreate(true)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));

      clusteredCacheManagerBuilder.build(true).close();
    }

    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .resourcePool("foo", 8, MemoryUnit.MB, "primary-server-resource")
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));
      PersistentCacheManager cm = clusteredCacheManagerBuilder.build(true);

      cm.close();
      cm.destroy();
    }
  }

  @Test
  public void testMissingPool() throws CachePersistenceException {
    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .autoCreate(true)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));

      clusteredCacheManagerBuilder.build(true).close();
    }

    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));
      PersistentCacheManager cm = clusteredCacheManagerBuilder.build(true);

      cm.close();
      cm.destroy();
    }
  }

  @Test
  public void testEmptyConfiguration() throws CachePersistenceException {
    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .autoCreate(true)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));

      clusteredCacheManagerBuilder.build(true).close();
    }

    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI));
      PersistentCacheManager cm = clusteredCacheManagerBuilder.build(true);

      cm.close();
      cm.destroy();
    }
  }

  @Test
  public void testMismatchedPoolSizesConfiguration() throws CachePersistenceException {
    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .autoCreate(true)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));

      clusteredCacheManagerBuilder.build(true).close();
    }

    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 9, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));
      PersistentCacheManager cm = clusteredCacheManagerBuilder.build(false);

      try {
        cm.init();
        fail("Expected StateTransitionException");
      } catch (StateTransitionException e) {
        //expected
      } finally {
        cm.destroy();
      }
    }
  }

  @Test
  public void testMismatchedDefaultPool() throws CachePersistenceException {
    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .autoCreate(true)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));

      clusteredCacheManagerBuilder.build(true).close();
    }

    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .defaultServerResource("secondary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB, "primary-server-resource")
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));
      PersistentCacheManager cm = clusteredCacheManagerBuilder.build(false);

      try {
        cm.init();
        fail("Expected StateTransitionException");
      } catch (StateTransitionException e) {
        //expected
      } finally {
        cm.destroy();
      }
    }
  }

  @Test
  public void testExtraPool() throws CachePersistenceException {
    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .autoCreate(true)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource"));

      clusteredCacheManagerBuilder.build(true).close();
    }

    {
      final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
          = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
                  .defaultServerResource("primary-server-resource")
                  .resourcePool("foo", 8, MemoryUnit.MB)
                  .resourcePool("bar", 8, MemoryUnit.MB, "secondary-server-resource")
                  .resourcePool("bat", 8, MemoryUnit.MB, "secondary-server-resource"));
      PersistentCacheManager cm = clusteredCacheManagerBuilder.build(false);

      try {
        cm.init();
        fail("Expected StateTransitionException");
      } catch (StateTransitionException e) {
        //expected
      } finally {
        cm.destroy();
      }
    }
  }

}
