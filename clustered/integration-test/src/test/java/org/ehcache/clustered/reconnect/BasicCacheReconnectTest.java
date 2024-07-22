/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.store.ReconnectInProgressException;
import org.ehcache.clustered.util.TCPProxyManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;
import org.terracotta.utilities.test.rules.TestRetryer;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofSeconds;
import static org.ehcache.testing.StandardCluster.clusterPath;
import static org.ehcache.testing.StandardCluster.leaseLength;
import static org.ehcache.testing.StandardCluster.newCluster;
import static org.ehcache.testing.StandardCluster.offheapResource;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.CLASS_RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.tryValues;

public class BasicCacheReconnectTest {

  private static TCPProxyManager proxyManager;
  private static PersistentCacheManager cacheManager;

  private static CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
          .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
          .build();

  @ClassRule @Rule
  public static final TestRetryer<Duration, Cluster> CLUSTER = tryValues(ofSeconds(1), ofSeconds(10), ofSeconds(30))
    .map(leaseLength -> newCluster().in(clusterPath()).withServiceFragment(
      offheapResource("primary-server-resource", 64) + leaseLength(leaseLength)).build())
    .outputIs(CLASS_RULE);

  @BeforeClass
  public static void initializeCacheManager() throws Exception {
    proxyManager = TCPProxyManager.create(CLUSTER.get().getConnectionURI());
    URI connectionURI = proxyManager.getURI();

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
              .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  @AfterClass
  public static void stopProxies() {
    proxyManager.close();
  }

  @Test
  public void cacheOpsDuringReconnection() throws Exception {

    try {

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

      CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
              ThreadLocalRandom.current()
                      .longs()
                      .forEach(value ->
                              cache.put(value, Long.toString(value))));

      expireLease();

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

      getSucceededFuture.get(20000, TimeUnit.MILLISECONDS);
    } finally {
      cacheManager.destroyCache("clustered-cache");
    }

  }

  @Test
  public void reconnectDuringCacheCreation() throws Exception {

    expireLease();

    Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

    assertThat(cache, notNullValue());

    cacheManager.destroyCache("clustered-cache");

  }

  @Test
  public void reconnectDuringCacheDestroy() throws Exception {

    Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

    assertThat(cache, notNullValue());

    expireLease();

    cacheManager.destroyCache("clustered-cache");
    assertThat(cacheManager.getCache("clustered-cache", Long.class, String.class), nullValue());

  }

  private void expireLease() throws InterruptedException {
    long delay = CLUSTER.input().plusSeconds(1L).toMillis();
    proxyManager.setDelay(delay);
    try {
      Thread.sleep(delay);
    } finally {
      proxyManager.setDelay(0);
    }
  }
}
