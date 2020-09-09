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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;
import org.terracotta.utilities.test.rules.TestRetryer;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static java.time.Duration.ofSeconds;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;
import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.CLASS_RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.tryValues;

public class CacheManagerDestroyReconnectTest extends ClusteredTests {


  private static PersistentCacheManager cacheManager;

  private static final List<TCPProxy> proxies = new ArrayList<>();

  @ClassRule @Rule
  public static final TestRetryer<Duration, Cluster> CLUSTER = tryValues(ofSeconds(1), ofSeconds(10), ofSeconds(30))
    .map(leaseLength -> newCluster().in(clusterPath()).withServiceFragment(
      offheapResource("primary-server-resource", 64) + leaseLength(leaseLength)).build())
    .outputIs(CLASS_RULE);

  @BeforeClass
  public static void initializeCacheManager() throws Exception {
    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.get().getConnectionURI(), proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
              .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  @Test
  public void testDestroyCacheManagerReconnects() throws Exception {

    long delay = CLUSTER.input().plusSeconds(1L).toMillis();
    setDelay(delay, proxies);
    try {
      Thread.sleep(delay);
    } finally {
      setDelay(0L, proxies);
    }

    cacheManager.close();

    cacheManager.destroy();

    System.out.println(cacheManager.getStatus());

  }

}
