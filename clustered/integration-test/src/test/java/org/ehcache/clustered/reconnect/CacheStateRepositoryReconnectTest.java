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
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.terracotta.testing.rules.Cluster;
import org.terracotta.utilities.test.rules.TestRetryer;

import java.io.Serializable;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.time.Duration.ofSeconds;
import static org.ehcache.testing.StandardCluster.clusterPath;
import static org.ehcache.testing.StandardCluster.leaseLength;
import static org.ehcache.testing.StandardCluster.newCluster;
import static org.ehcache.testing.StandardCluster.offheapResource;
import static org.ehcache.testing.StandardTimeouts.eventually;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.notNullValue;
import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.CLASS_RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.tryValues;

public class CacheStateRepositoryReconnectTest {
  private static TCPProxyManager proxyManager;
  private static PersistentCacheManager cacheManager;

  private static CacheConfiguration<NodeKey, NodeVal> config = CacheConfigurationBuilder
    .newCacheConfigurationBuilder(NodeKey.class, NodeVal.class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.
          clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
    .withResilienceStrategy(new ThrowingResiliencyStrategy<>()).build();

  @ClassRule
  @Rule
  public static final TestRetryer<Duration, Cluster> CLUSTER = tryValues(ofSeconds(1), ofSeconds(10), ofSeconds(30))
    .map(leaseLength -> newCluster().in(clusterPath())
      .withServiceFragment(offheapResource("primary-server-resource", 64) +
        leaseLength(leaseLength)).build()).outputIs(CLASS_RULE);

  @BeforeClass
  public static void initializeCacheManager() throws Exception {
    proxyManager = TCPProxyManager.create(CLUSTER.get().getConnectionURI());
    URI connectionURI = proxyManager.getURI();

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = CacheManagerBuilder
      .newCacheManagerBuilder().with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
        .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  @AfterClass
  public static void stopProxies() {
    proxyManager.close();
    cacheManager.close();
  }

  @Test
  public void cacheOpsDuringReconnection() throws Exception {
    try {
      Cache<NodeKey, NodeVal> cache = cacheManager.createCache("clustered-cache", config);

      expireLease();

      AtomicBoolean exceptionOccur = new AtomicBoolean(false);
      CompletableFuture<Void> putFuture = CompletableFuture.runAsync(() -> {
        while (true) {
          try {
            cache.put(new NodeKey(String.valueOf(1L)), new NodeVal(Long.toString(1L)));
            break;
          } catch (Exception e) {
            exceptionOccur.compareAndSet(false, true);
            assertThat(e.getCause().getCause(), instanceOf(ReconnectInProgressException.class));
          }
        }
      });

      assertThat(putFuture::isDone, eventually().is(true));
      assertThat(exceptionOccur.get(), is(true));

      NodeVal nodeVal = cache.get(new NodeKey(String.valueOf(1L)));
      assertThat(nodeVal.val, is("1"));
    } finally {
      cacheManager.destroyCache("clustered-cache");
    }
  }

  @Test
  public void reconnectDuringCacheCreation() throws Exception {
    try {
      expireLease();

      Cache<NodeKey, NodeVal> cache = cacheManager.createCache("clustered-cache", config);

      assertThat(cache, notNullValue());
    } finally {
      cacheManager.destroyCache("clustered-cache");
    }
  }

  @Test
  public void reconnectDuringCacheDestroy() throws Exception {
    try {
      Cache<NodeKey, NodeVal> cache = cacheManager.createCache("clustered-cache", config);

      assertThat(cache, notNullValue());

      expireLease();
    } finally {
      cacheManager.destroyCache("clustered-cache");
      assertThat(cacheManager.getCache("clustered-cache", NodeKey.class, NodeVal.class), nullValue());
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

  static class NodeKey implements Serializable {
    private static final long serialVersionUID = 2721130393821919318L;

    private String key;

    public NodeKey(String key) {
      this.key = key;
    }

    @Override
    public boolean equals(Object nk) {
      if (nk == null || nk.getClass() != getClass()) {
        return false;
      }
      NodeKey that = (NodeKey) nk;
      return that.key.equals(key);
    }

    @Override
    public int hashCode() {
      return key.hashCode();
    }
  }

  static class NodeVal implements Serializable {
    private static final long serialVersionUID = -1043724616650766155L;

    private String val;

    public NodeVal(String val) {
      this.val = val;
    }

    @Override
    public boolean equals(Object nk) {
      if (nk == null || nk.getClass() != getClass()) {
        return false;
      }
      NodeVal that = (NodeVal) nk;
      return that.val.equals(val);
    }

    @Override
    public int hashCode() {
      return val.hashCode();
    }
  }
}
