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
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.ehcache.clustered.reconnect.BasicCacheReconnectTest.RESOURCE_CONFIG;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class CacheManagerDestroyReconnectTest extends ClusteredTests {


  private static PersistentCacheManager cacheManager;

  private static final List<TCPProxy> proxies = new ArrayList<>();

  @ClassRule
  public static Cluster CLUSTER =
          newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();

    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.getConnectionURI(), proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"));
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  @Test
  public void testDestroyCacheManagerReconnects() throws Exception {

    setDelay(6000, proxies);
    Thread.sleep(6000);

    setDelay(0L, proxies);

    cacheManager.close();

    cacheManager.destroy();

    System.out.println(cacheManager.getStatus());

  }

}
