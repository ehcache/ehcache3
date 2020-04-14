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
import org.ehcache.clustered.TestRetryer;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static java.util.EnumSet.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class CacheManagerDestroyReconnectTest extends ClusteredTests {


  private static PersistentCacheManager cacheManager;

  private static final List<TCPProxy> proxies = new ArrayList<>();

  @ClassRule @Rule
  public static final TestRetryer<Integer, Cluster> CLUSTER = new TestRetryer<>(leaseLength -> newCluster()
    .in(clusterPath()).withServiceFragment(
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
        + "<ohr:offheap-resources>"
        + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
        + "</ohr:offheap-resources>"
        + "</config>\n"
        + "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
        + "<lease:connection-leasing>"
        + "<lease:lease-length unit='seconds'>" + leaseLength + "</lease:lease-length>"
        + "</lease:connection-leasing>"
        + "</service>")
    .build(), of(TestRetryer.OutputIs.CLASS_RULE), 1, 10, 30);

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getOutput().getClusterControl().waitForActive();

    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.getOutput().getConnectionURI(), proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
              .autoCreate(server -> server.defaultServerResource("primary-server-resource")));
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  @Test
  public void testDestroyCacheManagerReconnects() throws Exception {

    long delay = SECONDS.toMillis(CLUSTER.getInput() + 1);
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
