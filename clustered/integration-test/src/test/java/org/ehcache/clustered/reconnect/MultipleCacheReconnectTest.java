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
package org.ehcache.clustered.reconnect;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.store.ReconnectInProgressException;
import org.ehcache.clustered.util.TCPProxyManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;
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
import java.util.concurrent.TimeUnit;

import static java.time.Duration.ofSeconds;
import static org.ehcache.testing.StandardCluster.clusterPath;
import static org.ehcache.testing.StandardCluster.leaseLength;
import static org.ehcache.testing.StandardCluster.newCluster;
import static org.ehcache.testing.StandardCluster.offheapResource;
import static org.ehcache.testing.StandardTimeouts.eventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.fail;
import static org.junit.internal.matchers.ThrowableCauseMatcher.hasCause;
import static org.terracotta.utilities.test.matchers.ThrowsMatcher.threw;
import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.CLASS_RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.tryValues;

public class MultipleCacheReconnectTest {
  private static TCPProxyManager proxyManager;
  private static PersistentCacheManager cacheManager;

  private static CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(
    Long.class, String.class, ResourcePoolsBuilder.newResourcePoolsBuilder()
      .with(ClusteredResourcePoolBuilder.clusteredDedicated(
        "primary-server-resource", 1, MemoryUnit.MB)
      )).withResilienceStrategy(new ThrowingResiliencyStrategy<>()).build();

  @ClassRule
  @Rule
  public static final TestRetryer<Duration, Cluster> CLUSTER = tryValues(ofSeconds(1), ofSeconds(10), ofSeconds(30))
    .map(leaseLength ->
      newCluster().in(clusterPath())
        .withServiceFragment(offheapResource("primary-server-resource", 64) + leaseLength(leaseLength))
        .build())
    .outputIs(CLASS_RULE);

  @BeforeClass
  public static void initializeCacheManager() throws Exception {
    proxyManager = TCPProxyManager.create(CLUSTER.get().getConnectionURI());
    URI connectionURI = proxyManager.getURI();

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = CacheManagerBuilder
      .newCacheManagerBuilder().with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm")
      ).autoCreate(server -> server.defaultServerResource("primary-server-resource")));
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
      Cache<Long, String> cache1 = cacheManager.createCache("clustered-cache1", config);
      Cache<Long, String> cache2 = cacheManager.createCache("clustered-cache2", config);

      expireLease();

      CompletableFuture<Void> putFuture1 = CompletableFuture.runAsync(() -> {
        cache1.put(0L, "firstcache");
      });

      CompletableFuture<Void> putFuture2 = CompletableFuture.runAsync(() -> {
        cache2.put(0L, "secondcache");
      });

      assertThat(() -> putFuture1.get(5000, TimeUnit.MILLISECONDS),
        threw(Matchers.<Throwable>both(instanceOf(ExecutionException.class))
          .and(hasCause(hasCause(hasCause(hasCause(instanceOf(ReconnectInProgressException.class))))))));

      assertThat(() -> putFuture2.get(5000, TimeUnit.MILLISECONDS),
        threw(Matchers.<Throwable>both(instanceOf(ExecutionException.class))
          .and(hasCause(hasCause(hasCause(hasCause(instanceOf(ReconnectInProgressException.class))))))));

      CompletableFuture<Void> getSucceededFuture1 = CompletableFuture.runAsync(() -> {
        while (true) {
          try {
            cache1.get(1L);
            break;
          } catch (RuntimeException e) {

          }
        }
      });

      CompletableFuture<Void> getSucceededFuture2 = CompletableFuture.runAsync(() -> {
        while (true) {
          try {
            cache2.get(1L);
            break;
          } catch (RuntimeException e) {

          }
        }
      });

      assertThat(getSucceededFuture1::isDone, eventually().is(true));
      assertThat(getSucceededFuture1.isCompletedExceptionally(), is(false));
      assertThat(getSucceededFuture2::isDone, eventually().is(true));
      assertThat(getSucceededFuture2.isCompletedExceptionally(), is(false));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void cacheClosedDuringReconnect() throws Exception {
    try {
      Cache<Long, String> cache1 = cacheManager.createCache("clustered-cache1", config);
      Cache<Long, String> cache2 = cacheManager.createCache("clustered-cache2", config);
      assertThat(cache1, notNullValue());
      assertThat(cache2, notNullValue());

      cache1.put(0L, "firstValue");
      cache2.put(0L, "secondValue");
      expireLease();

      assertThat(() -> cache1.get(0L),
        threw(Matchers.<Throwable>both(instanceOf(RuntimeException.class))
          .and(hasCause(hasCause(hasCause(instanceOf(ReconnectInProgressException.class)))))));

      cacheManager.removeCache("clustered-cache1");

      try {
        cache1.get(0L);
        fail();
      } catch (RuntimeException e) {
        assertThat(e, instanceOf(IllegalStateException.class));
      }

      CompletableFuture<Void> getSucceededFuture2 = CompletableFuture.runAsync(() -> {
        while (true) {
          try {
            cache2.get(0L);
            break;
          } catch (RuntimeException e) {

          }
        }
      });

      assertThat(getSucceededFuture2::isDone, eventually().is(true));
      assertThat(getSucceededFuture2.isCompletedExceptionally(), is(false));
    } finally {
      cacheManager.close();
    }
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
