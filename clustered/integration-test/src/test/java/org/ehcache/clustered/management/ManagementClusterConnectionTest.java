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
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.utilities.test.rules.TestRetryer;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.unmodifiableMap;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.clustered.management.AbstractClusteringManagementTest.waitForAllNotifications;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.testing.StandardTimeouts.eventually;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;
import static org.terracotta.utilities.test.rules.TestRetryer.OutputIs.CLASS_RULE;
import static org.terracotta.utilities.test.rules.TestRetryer.tryValues;

public class ManagementClusterConnectionTest extends ClusteredTests {

  protected static CacheManager cacheManager;
  protected static ObjectMapper mapper = new ObjectMapper();

  private static final List<TCPProxy> proxies = new ArrayList<>();
  private static final Map<String, Long> resources;
  static {
    HashMap<String, Long> map = new HashMap<>();
    map.put("primary-server-resource", 64L);
    map.put("secondary-server-resource", 64L);
    resources = unmodifiableMap(map);
  }

  @ClassRule @Rule
  public static TestRetryer<Duration, ClusterWithManagement> CLUSTER = tryValues(ofSeconds(1), ofSeconds(10), ofSeconds(30))
    .map(leaseLength -> new ClusterWithManagement(
      newCluster().in(clusterPath()).withServiceFragment(
        offheapResources(resources) + leaseLength(leaseLength)).build()))
    .outputIs(CLASS_RULE);

  @BeforeClass
  public static void beforeClass() throws Exception {

    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    CLUSTER.get().getCluster().getClusterControl().waitForActive();

    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.get().getCluster().getConnectionURI(), proxies);

    cacheManager = newCacheManagerBuilder()
            // cluster config
            .with(cluster(connectionURI.resolve("/my-server-entity-1"))
                  .autoCreate(server -> server
                    .defaultServerResource("primary-server-resource")
                    .resourcePool("resource-pool-a", 10, MemoryUnit.MB, "secondary-server-resource") // <2>
                    .resourcePool("resource-pool-b", 10, MemoryUnit.MB))) // will take from primary-server-resource
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
    waitForAllNotifications(CLUSTER.get().getNmsService(),
            "CLIENT_CONNECTED",
            "CLIENT_PROPERTY_ADDED",
            "CLIENT_PROPERTY_ADDED",
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
  }

  @Test
  public void test_reconnection() throws Exception {
    long count = CLUSTER.get().getNmsService().readTopology().clientStream()
            .filter(client -> client.getName()
                    .startsWith("Ehcache:") && client.isManageable() && client.getTags()
                    .containsAll(Arrays.asList("webapp-1", "server-node-1")))
            .count();

    Assert.assertThat(count, Matchers.equalTo(1L));

    String instanceId = getInstanceId();

    long delay = CLUSTER.input().plusSeconds(1L).toMillis();
    setDelay(delay, proxies);
    try {
      Thread.sleep(delay);
    } finally {
      setDelay(0L, proxies);
    }

    Cache<String, String> cache = cacheManager.getCache("dedicated-cache-1", String.class, String.class);
    String initiate_reconnect = cache.get("initiate reconnect");

    assertThat(initiate_reconnect, Matchers.nullValue());

    assertThat(() -> {
      try {
        return CLUSTER.get().getNmsService().readTopology().clientStream()
                  .filter(client -> client.getName()
                          .startsWith("Ehcache:") && client.isManageable() && client.getTags()
                          .containsAll(Arrays.asList("webapp-1", "server-node-1")))
                  .count();
      } catch (Exception e) {
        throw new AssertionError(e);
      }
    }, eventually().is(1L));
    assertThat(getInstanceId(), equalTo(instanceId));
  }

  private String getInstanceId() throws Exception {
    return CLUSTER.get().getNmsService().readTopology().clientStream()
      .filter(client -> client.getName().startsWith("Ehcache:") && client.isManageable())
      .findFirst().get()
      .getManagementRegistry().get()
      .getCapability("SettingsCapability").get()
      .getDescriptors(Settings.class).stream()
      .filter(settings -> settings.containsKey("instanceId"))
      .map(settings -> settings.getString("instanceId"))
      .findFirst().get();
  }
}
