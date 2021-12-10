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
import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.util.BeforeAll;
import org.ehcache.clustered.util.BeforeAllRule;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.entity.nms.client.NmsService;
import org.terracotta.management.model.cluster.AbstractManageableNode;
import org.terracotta.management.model.cluster.Client;
import org.terracotta.management.model.cluster.ClientIdentifier;
import org.terracotta.management.model.cluster.ServerEntity;
import org.terracotta.management.model.cluster.ServerEntityIdentifier;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static java.util.Collections.unmodifiableMap;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredShared;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.rules.RuleChain.outerRule;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

@SuppressWarnings("rawtypes") // Need to suppress because of a Javac bug giving a rawtype on AbstractManageableNode::isManageable.
public abstract class AbstractClusteringManagementTest extends ClusteredTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClusteringManagementTest.class);

  private static final Map<String, Long> resources;
  static {
    HashMap<String, Long> map = new HashMap<>();
    map.put("primary-server-resource", 64L);
    map.put("secondary-server-resource", 64L);
    resources = unmodifiableMap(map);
  }

  protected static CacheManager cacheManager;
  protected static ClientIdentifier ehcacheClientIdentifier;
  protected static ServerEntityIdentifier clusterTierManagerEntityIdentifier;
  protected static final ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);

  @ClassRule
  public static final ClusterWithManagement CLUSTER = new ClusterWithManagement(newCluster(2)
    .in(clusterPath()).withServiceFragment(offheapResources(resources)).build());

  @Rule
  public final RuleChain rules = outerRule(Timeout.seconds(90)).around(new BeforeAllRule(this));

  @BeforeAll
  public void beforeAllTests() throws Exception {
    initCM();
    initIdentifiers();
  }

  @Before
  public void init() {
    CLUSTER.getNmsService().readMessages();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    tearDownCacheManagerAndStatsCollector();
  }

  private static void initCM() throws InterruptedException {
    cacheManager = newCacheManagerBuilder()
      // cluster config
      .with(cluster(CLUSTER.getCluster().getConnectionURI().resolve("/my-server-entity-1"))
        .autoCreate(server -> server
          .defaultServerResource("primary-server-resource")
          .resourcePool("resource-pool-a", 10, MemoryUnit.MB, "secondary-server-resource") // <2>
          .resourcePool("resource-pool-b", 8, MemoryUnit.MB))) // will take from primary-server-resource
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
      .withCache("shared-cache-2", newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .with(clusteredShared("resource-pool-a")))
        .build())
      .withCache("shared-cache-3", newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .with(clusteredShared("resource-pool-b")))
        .build())
      .build(true);

    // ensure the CM is running and get its client id
    assertThat(cacheManager.getStatus(), equalTo(Status.AVAILABLE));

    // test_notifs_sent_at_CM_init
    waitForAllNotifications(
      "CLIENT_CONNECTED",
      "CLIENT_PROPERTY_ADDED",
      "CLIENT_PROPERTY_ADDED",
      "CLIENT_REGISTRY_AVAILABLE",
      "CLIENT_TAGS_UPDATED",
      "EHCACHE_RESOURCE_POOLS_CONFIGURED",
      "EHCACHE_SERVER_STORE_CREATED", "EHCACHE_SERVER_STORE_CREATED", "EHCACHE_SERVER_STORE_CREATED",
      "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE",
      "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED",
      "SERVER_ENTITY_DESTROYED",
      "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED", "SERVER_ENTITY_FETCHED",
      "SERVER_ENTITY_UNFETCHED",
      "EHCACHE_RESOURCE_POOLS_CONFIGURED",

      "SERVER_ENTITY_DESTROYED",
      "SERVER_ENTITY_CREATED",
      "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED", "SERVER_ENTITY_CREATED",
      "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_AVAILABLE",
      "EHCACHE_SERVER_STORE_CREATED", "EHCACHE_SERVER_STORE_CREATED", "EHCACHE_SERVER_STORE_CREATED"

    );
  }

  private static void initIdentifiers() throws Exception {
    while ((ehcacheClientIdentifier = readTopology().getClients().values()
        .stream()
        .filter(client -> client.getName().equals("Ehcache:my-server-entity-1"))
        .filter(AbstractManageableNode::isManageable)
        .findFirst()
        .map(Client::getClientIdentifier)
        .orElse(null)) == null) {
      sleep(200);
    }

    while ((clusterTierManagerEntityIdentifier = readTopology()
        .activeServerEntityStream()
        .filter(serverEntity -> serverEntity.getName().equals("my-server-entity-1"))
        .filter(AbstractManageableNode::isManageable)
        .map(ServerEntity::getServerEntityIdentifier)
        .findFirst()
        .orElse(null)) == null) {
      sleep(200);
    }
  }

  private static void tearDownCacheManagerAndStatsCollector() throws Exception {
    if (cacheManager != null && cacheManager.getStatus() == Status.AVAILABLE) {

      readTopology().getClient(ehcacheClientIdentifier)
        .ifPresent(client -> {
          try {
            CLUSTER.getNmsService().stopStatisticCollector(client.getContext().with("cacheManagerName", "my-super-cache-manager")).waitForReturn();
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

      cacheManager.close();
    }
  }

  public static org.terracotta.management.model.cluster.Cluster readTopology() throws Exception {
    org.terracotta.management.model.cluster.Cluster cluster = CLUSTER.getNmsService().readTopology();
    //System.out.println(mapper.writeValueAsString(cluster.toMap()));
    return cluster;
  }

  public static void sendManagementCallOnClientToCollectStats() throws Exception {
    org.terracotta.management.model.cluster.Cluster topology = readTopology();
    Client manageableClient = topology.getClient(ehcacheClientIdentifier).filter(AbstractManageableNode::isManageable).get();
    Context cmContext = manageableClient.getContext()
      .with("cacheManagerName", "my-super-cache-manager");
    CLUSTER.getNmsService().startStatisticCollector(cmContext, 1, TimeUnit.SECONDS).waitForReturn();
  }

  public static List<ContextualStatistics> waitForNextStats() throws Exception {
    // uses the monitoring to get the content of the stat buffer when some stats are collected
    return CLUSTER.getNmsService().waitForMessage(message -> message.getType().equals("STATISTICS"))
      .stream()
      .filter(message -> message.getType().equals("STATISTICS"))
      .flatMap(message -> message.unwrap(ContextualStatistics.class).stream())
      .collect(Collectors.toList());
  }

  protected static String read(String path) {
    try (Scanner scanner = new Scanner(AbstractClusteringManagementTest.class.getResourceAsStream(path), "UTF-8")) {
      return scanner.useDelimiter("\\A").next();
    }
  }

  protected static String normalizeForLineEndings(String stringToNormalize) {
    return stringToNormalize.replace("\r\n", "\n").replace("\r", "\n");
  }

  public static void waitForAllNotifications(String... notificationTypes) throws InterruptedException {
    waitForAllNotifications(CLUSTER.getNmsService(), notificationTypes);
  }

  public static void waitForAllNotifications(NmsService nmsService, String... notificationTypes) throws InterruptedException {
    List<String> waitingFor = new ArrayList<>(Arrays.asList(notificationTypes));
    List<ContextualNotification> missingOnes = new ArrayList<>();
    List<ContextualNotification> existingOnes = new ArrayList<>();

    // please keep these sout because it is really hard to troubleshoot blocking tests in the beforeClass method in the case we do not receive all notifs.
//    System.out.println("waitForAllNotifications: " + waitingFor);

    Thread t = new Thread(() -> {
      try {
        nmsService.waitForMessage(message -> {
          if (message.getType().equals("NOTIFICATION")) {
            for (ContextualNotification notification : message.unwrap(ContextualNotification.class)) {
              if ("org.terracotta.management.entity.nms.client.NmsEntity".equals(notification.getContext().get("entityType"))) {
                LOGGER.info("IGNORE:" + notification); // this is the passive NmsEntity, sometimes we catch it, sometimes not
              } else if (waitingFor.remove(notification.getType())) {
                existingOnes.add(notification);
                LOGGER.debug("Remove " + notification);
                LOGGER.debug("Still waiting for: " + waitingFor);
              } else {
                LOGGER.debug("Extra: " + notification);
                missingOnes.add(notification);
              }
            }
          }
          return waitingFor.isEmpty();
        });
      } catch (InterruptedException e) {
        // Get out
      }
    });
    t.start();
    t.join(30_000); // should be way enough to receive all messages
    t.interrupt(); // we interrupt the thread that is waiting on the message queue

    assertTrue("Still waiting for: " + waitingFor + ", only got: " + existingOnes, waitingFor.isEmpty());
    assertTrue("Unexpected notification: " + missingOnes + ", only got: " + existingOnes, missingOnes.isEmpty());
  }
}
