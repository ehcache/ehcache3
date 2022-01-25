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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

@RunWith(Parameterized.class)
public class LeaseTest extends ClusteredTests {

  private static final String RESOURCE_CONFIG =
          "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
                  + "<ohr:offheap-resources>"
                  + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
                  + "</ohr:offheap-resources>"
                  + "</config>\n"
                  + "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
                  + "<lease:connection-leasing>"
                  + "<lease:lease-length unit='seconds'>5</lease:lease-length>"
                  + "</lease:connection-leasing>"
                  + "</service>";

  @ClassRule
  public static Cluster CLUSTER =
          newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  private final List<TCPProxy> proxies = new ArrayList<>();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

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
    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.getConnectionURI(), proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
                    .timeouts(TimeoutsBuilder.timeouts()
                            .connection(Duration.ofSeconds(20)))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"));
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

    setDelay(6000, proxies);
    Thread.sleep(6000);
    // We will now have lost the lease

    setDelay(0L, proxies);

    AtomicBoolean timedout = new AtomicBoolean(false);

    CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
      while (!timedout.get()) {
        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          throw new AssertionError(e);
        }
        String result = cache.get(1L);
        if (result != null) {
          return result;
        }
      }
      return null;
    });

    assertThat(future.get(30, TimeUnit.SECONDS), is("The one"));

    timedout.set(true);

    assertThat(cache.get(2L), equalTo("The two"));
    assertThat(cache.get(3L), equalTo("The three"));

  }

}
