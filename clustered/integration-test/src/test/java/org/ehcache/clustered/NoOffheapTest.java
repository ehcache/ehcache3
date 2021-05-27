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

import org.ehcache.StateTransitionException;
import org.ehcache.config.units.MemoryUnit;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import static java.util.function.UnaryOperator.identity;
import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.junit.Assert.fail;


public class NoOffheapTest extends ClusteredTests {

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(clusterPath()).build();

  @Test
  public void testNoOffheap() throws InterruptedException {
    try {
      newCacheManagerBuilder().with(cluster(CLUSTER.getConnectionURI().resolve("/no-offheap-cm"))
        .autoCreate(identity()))
        .withCache("testNoOffheap", newCacheConfigurationBuilder(Long.class, byte[].class, newResourcePoolsBuilder()
          .with(clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB))
        )).build(true).close();
      fail();
    } catch (StateTransitionException e) {
      assertThat(e).hasMessage("Could not create the cluster tier manager 'no-offheap-cm'.");
    }
  }
}
