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
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.terracotta.connection.Connection;
import org.terracotta.management.entity.tms.TmsAgentConfig;
import org.terracotta.management.entity.tms.client.TmsAgentEntity;
import org.terracotta.management.entity.tms.client.TmsAgentEntityFactory;
import org.terracotta.management.entity.tms.client.TmsAgentService;
import org.terracotta.management.model.cluster.Client;
import org.terracotta.management.model.cluster.ClientIdentifier;
import org.terracotta.management.model.cluster.ServerEntityIdentifier;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.registry.collect.StatisticConfiguration;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredShared;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

public abstract class AbstractClusteringManagementTest {

  private static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "<ohr:resource name=\"secondary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  protected static CacheManager cacheManager;
  protected static ClientIdentifier ehcacheClientIdentifier;
  protected static ServerEntityIdentifier ehcacheServerEntityIdentifier;
  protected static ObjectMapper mapper = new ObjectMapper();

  protected static TmsAgentService tmsAgentService;
  protected static ServerEntityIdentifier tmsServerEntityIdentifier;

  private static final List<File> MANAGEMENT_PLUGINS = System.getProperty("managementPlugins") == null ?
    Collections.emptyList() :
    Stream.of(System.getProperty("managementPlugins").split(File.pathSeparator))
      .map(File::new)
      .collect(Collectors.toList());

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("build/cluster"), 1, MANAGEMENT_PLUGINS, "", RESOURCE_CONFIG, "");

  @BeforeClass
  public static void beforeClass() throws Exception {
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    CLUSTER.getClusterControl().waitForActive();

    // simulate a TMS client
    Connection managementConnection = CLUSTER.newConnection();
    TmsAgentEntityFactory entityFactory = new TmsAgentEntityFactory(managementConnection, AbstractClusteringManagementTest.class.getName());
    TmsAgentEntity tmsAgentEntity = entityFactory.retrieveOrCreate(new TmsAgentConfig()
      .setStatisticConfiguration(new StatisticConfiguration(
        60, SECONDS,
        100, 1, SECONDS,
        10, SECONDS
      )));
    tmsAgentService = new TmsAgentService(tmsAgentEntity);
    tmsAgentService.setOperationTimeout(5, TimeUnit.SECONDS);

    tmsServerEntityIdentifier = readTopology()
      .activeServerEntityStream()
      .filter(serverEntity -> serverEntity.getType().equals(TmsAgentConfig.ENTITY_TYPE))
      .findFirst()
      .get() // throws if not found
      .getServerEntityIdentifier();

    cacheManager = newCacheManagerBuilder()
      // cluster config
      .with(cluster(CLUSTER.getConnectionURI().resolve("/my-server-entity-1"))
        .autoCreate()
        .defaultServerResource("primary-server-resource")
        .resourcePool("resource-pool-a", 28, MemoryUnit.MB, "secondary-server-resource") // <2>
        .resourcePool("resource-pool-b", 16, MemoryUnit.MB)) // will take from primary-server-resource
      // management config
      .using(new DefaultManagementRegistryConfiguration()
        .addTags("webapp-1", "server-node-1")
        .setCacheManagerAlias("my-super-cache-manager")
        .addConfiguration(new EhcacheStatisticsProviderConfiguration(
          1, TimeUnit.MINUTES,
          100, 1, TimeUnit.SECONDS,
          10, TimeUnit.SECONDS)))
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
    ehcacheClientIdentifier = readTopology().getClients().values()
      .stream()
      .filter(client -> client.getName().equals("Ehcache:my-server-entity-1"))
      .findFirst()
      .map(Client::getClientIdentifier)
      .get();

    ehcacheServerEntityIdentifier = readTopology()
      .activeServerEntityStream()
      .filter(serverEntity -> serverEntity.getName().equals("my-server-entity-1"))
      .findFirst()
      .get() // throws if not found
      .getServerEntityIdentifier();

    // test_notifs_sent_at_CM_init
    List<Message> messages = readMessages();
    List<String> notificationTypes = notificationTypes(messages);

    Map<String, List<String>> counts = notificationTypes.stream().collect(Collectors.groupingBy(o -> o));
    assertThat(counts.keySet(), hasSize(12));
    assertThat(counts.get("CLIENT_CONNECTED"), hasSize(1));
    assertThat(counts.get("CLIENT_REGISTRY_AVAILABLE"), hasSize(1));
    assertThat(counts.get("CLIENT_TAGS_UPDATED"), hasSize(1));
    assertThat(counts.get("EHCACHE_CLIENT_VALIDATED"), hasSize(1));
    assertThat(counts.get("EHCACHE_RESOURCE_POOLS_CONFIGURED"), hasSize(1));
    assertThat(counts.get("EHCACHE_SERVER_STORE_CREATED"), hasSize(3));
    assertThat(counts.get("ENTITY_REGISTRY_AVAILABLE"), hasSize(2));
    assertThat(counts.get("ENTITY_REGISTRY_UPDATED"), hasSize(11));
    assertThat(counts.get("SERVER_ENTITY_CREATED"), hasSize(5));
    assertThat(counts.get("SERVER_ENTITY_DESTROYED"), hasSize(1));
    assertThat(counts.get("SERVER_ENTITY_FETCHED"), hasSize(7));
    assertThat(counts.get("SERVER_ENTITY_UNFETCHED"), hasSize(3));

    assertThat(readMessages(), hasSize(0));

    sendManagementCallOnEntityToCollectStats();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (cacheManager != null && cacheManager.getStatus() == Status.AVAILABLE) {
      cacheManager.close();
    }
  }

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(60);

  @Before
  public void init() throws Exception {
    if (tmsAgentService != null) {
      readMessages();
    }
  }

  protected static org.terracotta.management.model.cluster.Cluster readTopology() throws Exception {
    return tmsAgentService.readTopology();
  }

  protected static List<Message> readMessages() throws Exception {
    return tmsAgentService.readMessages();
  }

  protected static void sendManagementCallOnClientToCollectStats(String... statNames) throws Exception {
    Context ehcacheClient = readTopology().getClient(ehcacheClientIdentifier).get().getContext()
      .with("cacheManagerName", "my-super-cache-manager");
    tmsAgentService.updateCollectedStatistics(ehcacheClient, "StatisticsCapability", asList(statNames)).waitForReturn();
  }

  protected static List<ContextualStatistics> waitForNextStats() throws Exception {
    // uses the monitoring consumre entity to get the content of the stat buffer when some stats are collected
    while (!Thread.currentThread().isInterrupted()) {
      List<ContextualStatistics> messages = readMessages()
        .stream()
        .filter(message -> message.getType().equals("STATISTICS"))
        .flatMap(message -> message.unwrap(ContextualStatistics.class).stream())
        .collect(Collectors.toList());
      if (messages.isEmpty()) {
        Thread.yield();
      } else {
        return messages;
      }
    }
    return Collections.emptyList();
  }

  protected static List<String> messageTypes(List<Message> messages) {
    return messages.stream().map(Message::getType).collect(Collectors.toList());
  }

  protected static List<String> notificationTypes(List<Message> messages) {
    return messages
      .stream()
      .filter(message -> "NOTIFICATION".equals(message.getType()))
      .flatMap(message -> message.unwrap(ContextualNotification.class).stream())
      .map(ContextualNotification::getType)
      .collect(Collectors.toList());
  }

  protected static String read(String path) throws FileNotFoundException {
    Scanner scanner = new Scanner(AbstractClusteringManagementTest.class.getResourceAsStream(path), "UTF-8");
    try {
      return scanner.useDelimiter("\\A").next();
    } finally {
      scanner.close();
    }
  }

  protected static String normalizeForLineEndings(String stringToNormalize) {
    return stringToNormalize.replace("\r\n", "\n").replace("\r", "\n");
  }

  private static void sendManagementCallOnEntityToCollectStats() throws Exception {
    Context context = readTopology().getSingleStripe().getActiveServerEntity(tmsServerEntityIdentifier).get().getContext();
    tmsAgentService.updateCollectedStatistics(context, "PoolStatistics", asList("Pool:AllocatedSize")).waitForReturn();
    tmsAgentService.updateCollectedStatistics(context, "ServerStoreStatistics", asList(
      "Store:AllocatedMemory",
      "Store:DataAllocatedMemory",
      "Store:OccupiedMemory",
      "Store:DataOccupiedMemory",
      "Store:Entries",
      "Store:UsedSlotCount",
      "Store:DataVitalMemory",
      "Store:VitalMemory",
      "Store:ReprobeLength",
      "Store:RemovedSlotCount",
      "Store:DataSize",
      "Store:TableCapacity"
    )).waitForReturn();
    tmsAgentService.updateCollectedStatistics(context, "OffHeapResourceStatistics", asList("OffHeapResource:AllocatedMemory")).waitForReturn();
  }

}
