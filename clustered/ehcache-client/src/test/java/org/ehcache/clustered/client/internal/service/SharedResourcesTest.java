/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

package org.ehcache.clustered.client.internal.service;

import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.config.units.MemoryUnit;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class SharedResourcesTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/");

  @Before
  public void definePassthroughServer() {
    UnitTestConnectionService.add(CLUSTER_URI,
      new UnitTestConnectionService.PassthroughServerBuilder()
        .resource("primary-server-resource", 64, MemoryUnit.MB)
        .build());
  }

  @After
  public void removePassthroughServer() {
    try {
      UnitTestConnectionService.remove(CLUSTER_URI);
    } catch (IllegalStateException e) {
      assertThat(e.getMessage(), is("Connection already closed"));
    }
  }

  @Test
  public void testUnsupportedSharedClusteredResource() throws Exception {
    try (PersistentCacheManager cm = newCacheManagerBuilder()
      .with(cluster(URI.create("terracotta://example.com:9540/")).autoCreate(c -> c))
      .sharedResources(newResourcePoolsBuilder().heap(10, MB).offheap(20, MB)
        .with(clusteredDedicated("primary-server-resource", 30, MemoryUnit.MB))) // not supported
      .build(true)) {
      cm.createCache("clustered-cache", newCacheConfigurationBuilder(Long.class, Long.class,
        newResourcePoolsBuilder().sharedHeap().sharedOffheap()
          .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB))));
        Assert.fail("Expected StateTransitionException");
    } catch (StateTransitionException e) {
      assertThat(e.getMessage(), Matchers.containsString("No Store.ElementalProvider types found to handle configuration"));
    }
  }
}
