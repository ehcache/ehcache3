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
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.common.PortChooser;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class LeaseTest extends ClusteredTests {
  private static final String STRIPE_SEPARATOR = ",";

  private static final String RESOURCE_CONFIG =
          "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
                  + "<ohr:offheap-resources>"
                  + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
                  + "</ohr:offheap-resources>" +
                  "</config>\n";

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

  @Test
  public void leaseExpiry() throws Exception {
    URI connectionURI = getProxyURI();

    final CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource"));
    final PersistentCacheManager cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();

    try {
      CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
              ResourcePoolsBuilder.newResourcePoolsBuilder()
                      .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB))).build();

      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);
      cache.put(1L, "The one");
      cache.put(2L, "The two");
      cache.put(3L, "The three");
      assertThat(cache.get(1L), equalTo("The one"));
      assertThat(cache.get(2L), equalTo("The two"));
      assertThat(cache.get(3L), equalTo("The three"));

      setDelay(6000);
      Thread.sleep(6000);
      // We will now have lost the lease

      setDelay(0L);

      assertThat(cache.get(1L), is(nullValue()));
      assertThat(cache.get(2L), is(nullValue()));
      assertThat(cache.get(3L), is(nullValue()));
    } finally {
      cacheManager.close();
    }

  }

  private void setDelay(long delay) {
    for (TCPProxy proxy : proxies) {
      proxy.setDelay(delay);
    }
  }

  private URI getProxyURI() throws Exception {
    URI connectionURI = CLUSTER.getConnectionURI();

    List<Integer> ports = parsePorts(connectionURI);
    List<Integer> proxyPorts = createProxyPorts(ports.size());

    for (int i = 0; i < ports.size(); i++) {
      int port = ports.get(i);
      int proxyPort = proxyPorts.get(i);

      InetAddress host = InetAddress.getByName("localhost");
      TCPProxy proxy = new TCPProxy(proxyPort, host, port, 0L, false, null);
      proxies.add(proxy);
      proxy.start();
    }

    return createURI(proxyPorts);
  }

  private List<Integer> parsePorts(URI connectionURI) {
    String uriString = connectionURI.toString();
    String withoutProtocol = uriString.substring(13);
    String[] stripes = withoutProtocol.split(STRIPE_SEPARATOR);

    List<Integer> ports = new ArrayList<>(stripes.length);
    for (String stripe : stripes) {
      String portString = stripe.substring(stripe.indexOf(":") + 1);
      int port = Integer.parseInt(portString);
      ports.add(port);
    }

    return ports;
  }

  private List<Integer> createProxyPorts(int portCount) {
    PortChooser portChooser = new PortChooser();
    int firstProxyPort = portChooser.chooseRandomPorts(portCount);

    List<Integer> proxyPorts = new ArrayList<>(portCount);
    for (int i = 0; i < portCount; i++) {
      proxyPorts.add(firstProxyPort + i);
    }

    return proxyPorts;
  }

  private URI createURI(List<Integer> proxyPorts) {
    List<String> stripes = new ArrayList<>();

    for (int proxyPort : proxyPorts) {
      String stripe = "localhost:" + proxyPort;
      stripes.add(stripe);
    }

    String uri = "terracotta://" + String.join(STRIPE_SEPARATOR, stripes);
    return URI.create(uri);
  }
}