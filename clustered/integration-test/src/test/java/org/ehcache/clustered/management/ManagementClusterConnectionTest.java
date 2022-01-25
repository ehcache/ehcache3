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
package org.ehcache.clustered.management;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tc.net.proxy.TCPProxy;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.clustered.management.AbstractClusteringManagementTest.createNmsService;
import static org.ehcache.clustered.management.AbstractClusteringManagementTest.initIdentifiers;
import static org.ehcache.clustered.management.AbstractClusteringManagementTest.readTopology;
import static org.ehcache.clustered.management.AbstractClusteringManagementTest.sendManagementCallOnEntityToCollectStats;
import static org.ehcache.clustered.management.AbstractClusteringManagementTest.tearDownCacheManagerAndStatsCollector;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class ManagementClusterConnectionTest {

  private static final String RESOURCE_CONFIG =
          "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
                  + "<ohr:offheap-resources>"
                  + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
                  + "<ohr:resource name=\"secondary-server-resource\" unit=\"MB\">64</ohr:resource>"
                  + "</ohr:offheap-resources>"
                  + "</config>\n"
                  + "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
                  + "<lease:connection-leasing>"
                  + "<lease:lease-length unit='seconds'>5</lease:lease-length>"
                  + "</lease:connection-leasing>"
                  + "</service>";

  protected static CacheManager cacheManager;
  protected static ObjectMapper mapper = new ObjectMapper();

  private static final List<TCPProxy> proxies = new ArrayList<>();

  @ClassRule
  public static Cluster CLUSTER = newCluster()
          .in(new File("build/cluster"))
          .withServiceFragment(RESOURCE_CONFIG).build();


  @BeforeClass
  public static void beforeClass() throws Exception {

    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    CLUSTER.getClusterControl().waitForActive();

    // simulate a TMS client
    createNmsService(CLUSTER);

    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.getConnectionURI(), proxies);

    cacheManager = newCacheManagerBuilder()
            // cluster config
            .with(cluster(connectionURI.resolve("/my-server-entity-1"))
                    .autoCreate()
                    .defaultServerResource("primary-server-resource")
                    .resourcePool("resource-pool-a", 10, MemoryUnit.MB, "secondary-server-resource") // <2>
                    .resourcePool("resource-pool-b", 10, MemoryUnit.MB)) // will take from primary-server-resource
            // management config
            .using(new DefaultManagementRegistryConfiguration()
                    .addTags("webapp-1", "server-node-1")
                    .setCacheManagerAlias("my-super-cache-manager"))
            // cache config
            .withCache("dedicated-cache-1", newCacheConfigurationBuilder(
                    String.class, String.class,
                    newResourcePoolsBuilder()
                            .heap(10, EntryUnit.ENTRIES)
                            .offheap(1, MemoryUnit.MB)
                            .with(clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
                    .build())
            .build(true);

    // ensure the CM is running and get its client id
    assertThat(cacheManager.getStatus(), equalTo(Status.AVAILABLE));

    // test_notifs_sent_at_CM_init
    AbstractClusteringManagementTest.waitForAllNotifications(
            "CLIENT_CONNECTED",
            "CLIENT_REGISTRY_AVAILABLE",
            "CLIENT_TAGS_UPDATED",
            "EHCACHE_RESOURCE_POOLS_CONFIGURED",
            "EHCACHE_SERVER_STORE_CREATED",
            "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE",
            "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED",
            "SERVER_ENTITY_DESTROYED",
            "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED",
            "SERVER_ENTITY_UNFETCHED"
    );

    initIdentifiers();

    sendManagementCallOnEntityToCollectStats();
  }

  @Test
  public void test_reconnection() throws Exception {
    long count = readTopology().clientStream()
            .filter(client -> client.getName()
                    .startsWith("Ehcache:") && client.isManageable() && client.getTags()
                    .containsAll(Arrays.asList("webapp-1", "server-node-1")))
            .count();

    Assert.assertThat(count, Matchers.equalTo(1L));

    String instanceId = getInstanceId();

    setDelay(6000, proxies);
    Thread.sleep(6000);

    setDelay(0L, proxies);

    Cache<String, String> cache = cacheManager.getCache("dedicated-cache-1", String.class, String.class);
    String initiate_reconnect = cache.get("initiate reconnect");

    assertThat(initiate_reconnect, Matchers.nullValue());

    while (!Thread.currentThread().isInterrupted()) {
//      System.out.println(mapper.writeValueAsString(readTopology().toMap()));

      count = readTopology().clientStream()
              .filter(client -> client.getName()
                      .startsWith("Ehcache:") && client.isManageable() && client.getTags()
                      .containsAll(Arrays.asList("webapp-1", "server-node-1")))
              .count();

      if (count == 1) {
        break;
      } else {
        Thread.sleep(1_000);
      }
    }

    assertThat(Thread.currentThread().isInterrupted(), is(false));
    assertThat(getInstanceId(), equalTo(instanceId));
  }

  private String getInstanceId() throws Exception {
    return readTopology().clientStream()
      .filter(client -> client.getName().startsWith("Ehcache:") && client.isManageable())
      .findFirst().get()
      .getManagementRegistry().get()
      .getCapability("SettingsCapability").get()
      .getDescriptors(Settings.class).stream()
      .filter(settings -> settings.containsKey("instanceId"))
      .map(settings -> settings.getString("instanceId"))
      .findFirst().get();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    tearDownCacheManagerAndStatsCollector();
  }

}
