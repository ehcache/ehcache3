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
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntity;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLock;
import org.ehcache.clustered.client.service.EntityBusyException;
import org.ehcache.clustered.common.internal.ClusterTierManagerConfiguration;
import org.ehcache.clustered.reconnect.ThrowingResiliencyStrategy;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.lease.connection.LeasedConnectionFactory;
import org.terracotta.testing.rules.Cluster;

import com.tc.net.proxy.TCPProxy;
import org.terracotta.utilities.test.rules.TestRetryer;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static org.ehcache.clustered.common.EhcacheEntityVersion.ENTITY_VERSION;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.ehcache.testing.StandardTimeouts.eventually;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;
import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.CLASS_RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.tryValues;

/**
 * ReconnectDuringDestroyTest
 */
public class ReconnectDuringDestroyTest extends ClusteredTests {

  private static URI connectionURI;
  private static List<TCPProxy> proxies;
  PersistentCacheManager cacheManager;

  @ClassRule @Rule
  public static final TestRetryer<Duration, Cluster> CLUSTER = tryValues(ofSeconds(1), ofSeconds(10), ofSeconds(3))
    .map(leaseLength -> newCluster().in(clusterPath()).withServiceFragment(
      offheapResource("primary-server-resource", 64) + leaseLength(leaseLength)).build())
    .outputIs(CLASS_RULE);

  @BeforeClass
  public static void initializeProxy() throws Exception {
    proxies = new ArrayList<>();
    connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.get().getConnectionURI(), proxies);
  }

  @Before
  public void initializeCacheManager() {
    ClusteringServiceConfiguration clusteringConfiguration =
      ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
        .autoCreate(server -> server.defaultServerResource("primary-server-resource")).build();

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
      = CacheManagerBuilder.newCacheManagerBuilder().with(clusteringConfiguration);
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  /*
  This is to test the scenario in which reconnect happens while cache manager
  destruction is in progress. This test checks whether the cache manager
  gets destructed properly in the reconnect path once the connection is closed
  after the prepareForDestroy() call.
  */
  @Test
  public void reconnectDuringDestroyTest() throws Exception {
    cacheManager.close();
    Connection client = null;
    try {
      client = LeasedConnectionFactory.connect(connectionURI, new Properties());
      VoltronReadWriteLock voltronReadWriteLock = new VoltronReadWriteLock(client, "crud-cm");
      try (VoltronReadWriteLock.Hold localMaintenance = voltronReadWriteLock.tryWriteLock()) {
        if (localMaintenance == null) {
          throw new EntityBusyException("Unable to obtain maintenance lease for " + "crud-cm");
        }
        EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);
        try {
          ClusterTierManagerClientEntity entity = ref.fetchEntity(null);
          entity.prepareForDestroy();
          entity.close();
        } catch (EntityNotFoundException e) {
          Assert.fail();
        }
      }
      // For reconnection.
      long delay = CLUSTER.input().plusSeconds(1).toMillis();
      setDelay(delay, proxies);
      try {
        Thread.sleep(delay);
      } finally {
        setDelay(0L, proxies);
      }
      client = LeasedConnectionFactory.connect(connectionURI, new Properties());

      // For mimicking the cacheManager.destroy() in the reconnect path.
      voltronReadWriteLock = new VoltronReadWriteLock(client, "crud-cm");
      try (VoltronReadWriteLock.Hold localMaintenance = voltronReadWriteLock.tryWriteLock()) {
        if (localMaintenance == null) {
          throw new EntityBusyException("Unable to obtain maintenance lease for " + "crud-cm");
        }
        EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> ref = getEntityRef(client);
        try {
          ClusterTierManagerClientEntity entity = ref.fetchEntity(null);
          entity.prepareForDestroy();
          entity.close();
        } catch (EntityNotFoundException e) {
          Assert.fail("Unexpected exception " + e.getMessage());
        }
        if (!ref.destroy()) {
          Assert.fail("Unexpected exception while trying to destroy cache manager");
        }
      }
    } finally {
      client.close();
    }
  }

  @Test
  public void reconnectAfterDestroyOneOfTheCache() throws Exception {
    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.newResourcePoolsBuilder()
          .with(ClusteredResourcePoolBuilder.
            clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
        .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
        .build();
      Cache<Long, String> cache1 = cacheManager.createCache("clustered-cache-1", config);
      Cache<Long, String> cache2 = cacheManager.createCache("clustered-cache-2", config);
      cache1.put(1L, "The one");
      cache1.put(2L, "The two");
      cache2.put(1L, "The one");
      cache2.put(2L, "The two");
      cacheManager.destroyCache("clustered-cache-1");

      // For reconnection.
      long delay = CLUSTER.input().plusSeconds(1L).toMillis();
      setDelay(delay, proxies);
      try {
        Thread.sleep(delay);
      } finally {
        setDelay(0L, proxies);
      }

      Cache<Long, String> cache2Again = cacheManager.getCache("clustered-cache-2", Long.class, String.class);
      eventually().runsCleanly(() -> {
        assertThat(cache2Again.get(1L), equalTo("The one"));
        assertThat(cache2Again.get(2L), equalTo("The two"));
      });
      cache2Again.put(3L, "The three");
      assertThat(cache2Again.get(3L), equalTo("The three"));
    } finally {
      cacheManager.close();
    }
  }

  private EntityRef<ClusterTierManagerClientEntity, ClusterTierManagerConfiguration, Void> getEntityRef(Connection client) throws org.terracotta.exception.EntityNotProvidedException {
    return client.getEntityRef(ClusterTierManagerClientEntity.class, ENTITY_VERSION, "crud-cm");
  }
}
