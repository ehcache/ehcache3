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

import com.tc.net.proxy.TCPProxy;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.testing.rules.Cluster;
import org.terracotta.utilities.test.rules.TestRetryer;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.time.Duration.ofSeconds;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.testing.StandardTimeouts.eventually;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;
import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.CLASS_RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.tryValues;

@RunWith(Parameterized.class)
public class LeaseTest extends ClusteredTests {

  @ClassRule
  @Rule
  public static final TestRetryer<Duration, Cluster> CLUSTER = tryValues(ofSeconds(1), ofSeconds(10), ofSeconds(30))
    .map(leaseLength -> newCluster().in(clusterPath()).withServiceFragment(
      offheapResource("primary-server-resource", 64) + leaseLength(leaseLength)).build())
    .outputIs(CLASS_RULE);

  private final List<TCPProxy> proxies = new ArrayList<>();

  @After
  public void after() {
    proxies.forEach(TCPProxy::stop);
  }

  @Parameterized.Parameters
  public static ResourcePoolsBuilder[] data() {
    return new ResourcePoolsBuilder[]{
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)),
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .heap(10, EntryUnit.ENTRIES)
        .with(clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB))
    };
  }

  @Parameterized.Parameter
  public ResourcePoolsBuilder resourcePoolsBuilder;

  @Test
  public void leaseExpiry() throws Exception {
    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.get().getConnectionURI(), proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
        .timeouts(TimeoutsBuilder.timeouts().connection(Duration.ofSeconds(20)))
        .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
      resourcePoolsBuilder).build();

    Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
    cache.put(1L, "The one");
    cache.put(2L, "The two");
    cache.put(3L, "The three");
    assertThat(cache.get(1L), equalTo("The one"));
    assertThat(cache.get(2L), equalTo("The two"));
    assertThat(cache.get(3L), equalTo("The three"));

    long delay = CLUSTER.input().plusSeconds(1L).toMillis();
    setDelay(delay, proxies);
    try {
      Thread.sleep(delay);
    } finally {
      setDelay(0L, proxies);
    }

    eventually().runsCleanly(() -> {
      assertThat(cache.get(1L), equalTo("The one"));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));
    });
  }

}
