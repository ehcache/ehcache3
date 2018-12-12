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
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.management.entity.nms.NmsConfig;
import org.terracotta.management.entity.nms.client.DefaultNmsService;
import org.terracotta.management.entity.nms.client.NmsEntity;
import org.terracotta.management.entity.nms.client.NmsEntityFactory;
import org.terracotta.management.entity.nms.client.NmsService;
import org.terracotta.management.model.cluster.AbstractManageableNode;
import org.terracotta.management.model.cluster.Client;
import org.terracotta.management.model.cluster.ClientIdentifier;
import org.terracotta.management.model.cluster.ServerEntity;
import org.terracotta.management.model.cluster.ServerEntityIdentifier;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.lang.Thread.sleep;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredShared;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

@SuppressWarnings("rawtypes") // Need to suppress because of a Javac bug giving a rawtype on AbstractManageableNode::isManageable.
public abstract class AbstractClusteringManagementTest extends ClusteredTests {

  private static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "<ohr:resource name=\"secondary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  protected static CacheManager cacheManager;
  protected static ClientIdentifier ehcacheClientIdentifier;
  protected static ServerEntityIdentifier clusterTierManagerEntityIdentifier;
  protected static ObjectMapper mapper = new ObjectMapper();

  static NmsService nmsService;
  protected static ServerEntityIdentifier tmsServerEntityIdentifier;
  protected static Connection managementConnection;

  static {
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);
  }

  @ClassRule
  public static Cluster CLUSTER = newCluster(2)
    .in(new File("build/cluster"))
    .withServiceFragment(RESOURCE_CONFIG)
    .build();

  @Rule
  public final RuleChain rules = RuleChain.emptyRuleChain()
    .around(Timeout.seconds(90))
    .around(new BeforeAllRule(this));

  @BeforeAll
  public void beforeAllTests() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    // simulate a TMS client
    createNmsService();

    initCM();

    initIdentifiers();

    sendManagementCallOnEntityToCollectStats();
  }

  @Before
  public void init() {
    if (nmsService != null) {
      // this call clear the CURRENT arrived messages, but be aware that some other messages can arrive just after the drain
      nmsService.readMessages();
    }
  }

  @AfterClass
  public static void afterClass() throws Exception {
    tearDownCacheManagerAndStatsCollector();
  }

  protected void initCM() throws InterruptedException {
    cacheManager = newCacheManagerBuilder()
      // cluster config
      .with(cluster(CLUSTER.getConnectionURI().resolve("/my-server-entity-1"))
        .autoCreate()
        .defaultServerResource("primary-server-resource")
        .resourcePool("resource-pool-a", 10, MemoryUnit.MB, "secondary-server-resource") // <2>
        .resourcePool("resource-pool-b", 8, MemoryUnit.MB)) // will take from primary-server-resource
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

  public static void initIdentifiers() throws Exception {
    tmsServerEntityIdentifier = null;
    ehcacheClientIdentifier = null;
    clusterTierManagerEntityIdentifier = null;

    do {
      tmsServerEntityIdentifier = readTopology()
        .activeServerEntityStream()
        .filter(serverEntity -> serverEntity.getType().equals(NmsConfig.ENTITY_TYPE))
        .filter(AbstractManageableNode::isManageable)
        .map(ServerEntity::getServerEntityIdentifier)
        .findFirst()
        .orElse(null);
      sleep(500);
    } while (tmsServerEntityIdentifier == null && !Thread.currentThread().isInterrupted());

    do {
      ehcacheClientIdentifier = readTopology().getClients().values()
        .stream()
        .filter(client -> client.getName().equals("Ehcache:my-server-entity-1"))
        .filter(AbstractManageableNode::isManageable)
        .findFirst()
        .map(Client::getClientIdentifier)
        .orElse(null);
      sleep(500);
    } while (ehcacheClientIdentifier == null && !Thread.currentThread().isInterrupted());

    do {
      clusterTierManagerEntityIdentifier = readTopology()
        .activeServerEntityStream()
        .filter(serverEntity -> serverEntity.getName().equals("my-server-entity-1"))
        .filter(AbstractManageableNode::isManageable)
        .map(ServerEntity::getServerEntityIdentifier)
        .findFirst()
        .orElse(null);
      sleep(500);
    } while (clusterTierManagerEntityIdentifier == null && !Thread.currentThread().isInterrupted());
  }

  public static void tearDownCacheManagerAndStatsCollector() throws Exception {
    if (cacheManager != null && cacheManager.getStatus() == Status.AVAILABLE) {

      if (nmsService != null) {
        readTopology().getClient(ehcacheClientIdentifier)
          .ifPresent(client -> {
            try {
              nmsService.stopStatisticCollector(client.getContext().with("cacheManagerName", "my-super-cache-manager")).waitForReturn();
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          });
      }

      cacheManager.close();
    }

    if (nmsService != null) {
      readTopology().getSingleStripe().getActiveServerEntity(tmsServerEntityIdentifier)
        .ifPresent(client -> {
          try {
            nmsService.stopStatisticCollector(client.getContext());
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        });

      managementConnection.close();
    }
  }

  public static void createNmsService() throws ConnectionException, EntityConfigurationException {
    createNmsService(CLUSTER);
  }

  public static void createNmsService(Cluster cluster) throws ConnectionException, EntityConfigurationException {
    managementConnection = cluster.newConnection();

    NmsEntityFactory entityFactory = new NmsEntityFactory(managementConnection, AbstractClusteringManagementTest.class.getName());
    NmsEntity tmsAgentEntity = entityFactory.retrieveOrCreate(new NmsConfig());

    nmsService = new DefaultNmsService(tmsAgentEntity);
    nmsService.setOperationTimeout(5, TimeUnit.SECONDS);
  }

  public static org.terracotta.management.model.cluster.Cluster readTopology() throws Exception {
    org.terracotta.management.model.cluster.Cluster cluster = nmsService.readTopology();
    //System.out.println(mapper.writeValueAsString(cluster.toMap()));
    return cluster;
  }

  public static void sendManagementCallOnClientToCollectStats() throws Exception {
    org.terracotta.management.model.cluster.Cluster topology = readTopology();
    Client manageableClient = topology.getClient(ehcacheClientIdentifier).filter(AbstractManageableNode::isManageable).get();
    Context cmContext = manageableClient.getContext()
      .with("cacheManagerName", "my-super-cache-manager");
    nmsService.startStatisticCollector(cmContext, 1, TimeUnit.SECONDS).waitForReturn();
  }

  public static List<ContextualStatistics> waitForNextStats() throws Exception {
    // uses the monitoring to get the content of the stat buffer when some stats are collected
    return nmsService.waitForMessage(message -> message.getType().equals("STATISTICS"))
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

  public static void sendManagementCallOnEntityToCollectStats() throws Exception {
    org.terracotta.management.model.cluster.Cluster topology = readTopology();
    ServerEntity manageableEntity = topology.getSingleStripe().getActiveServerEntity(tmsServerEntityIdentifier).filter(AbstractManageableNode::isManageable).get();
    Context context = manageableEntity.getContext();
    nmsService.startStatisticCollector(context, 1, TimeUnit.SECONDS).waitForReturn();
  }

  public static void waitForAllNotifications(String... notificationTypes) throws InterruptedException {
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
              if (waitingFor.remove(notification.getType())) {
                existingOnes.add(notification);
//                System.out.println("Remove " + notification.getType());
//                System.out.println("Still waiting for: " + waitingFor);
              } else {
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
