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
package org.ehcache.clustered.reconnect;

import com.tc.net.proxy.TCPProxy;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.store.ReconnectInProgressException;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class BasicCacheReconnectTest {
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

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  private final List<TCPProxy> proxies = new ArrayList<>();

  @Test
  public void cacheOpsDuringReconnection() throws Exception {

    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.getConnectionURI(), proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"));
    PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
                    .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
            .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
            .build();

    Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

    CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
            ThreadLocalRandom.current()
            .longs()
            .forEach(value ->
                    cache.put(value, Long.toString(value))));

    setDelay(6000, proxies);
    Thread.sleep(6000);

    setDelay(0L, proxies);

    try {
      future.get(5000, TimeUnit.MILLISECONDS);
      fail();
    } catch (ExecutionException e) {
      assertThat(e.getCause().getCause().getCause(), instanceOf(ReconnectInProgressException.class));
    }

    CompletableFuture<Void> getSucceededFuture = CompletableFuture.runAsync(() -> {
      while (true) {
        try {
          cache.get(1L);
          break;
        } catch (RuntimeException e) {

        }
      }
    });

    getSucceededFuture.get(5000, TimeUnit.MILLISECONDS);

  }
}
