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
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.management.entity.management.ManagementAgentConfig;
import org.terracotta.management.entity.management.client.ManagementAgentEntityFactory;
import org.terracotta.management.entity.management.client.ManagementAgentService;
import org.terracotta.management.entity.monitoring.client.MonitoringServiceEntityFactory;
import org.terracotta.management.entity.monitoring.client.MonitoringServiceProxyEntity;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.call.Parameter;
import org.terracotta.management.model.cluster.Client;
import org.terracotta.management.model.cluster.ClientIdentifier;
import org.terracotta.management.model.cluster.ServerEntityIdentifier;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.model.stats.StatisticHistory;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredShared;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public abstract class AbstractClusteringManagementTest {

  private static final String RESOURCE_CONFIG =
    "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "<ohr:resource name=\"secondary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</service>\n";

  protected static MonitoringServiceProxyEntity consumer;
  protected static CacheManager cacheManager;
  protected static ClientIdentifier clientIdentifier;
  protected static ServerEntityIdentifier serverEntityIdentifier;
  protected static ObjectMapper mapper = new ObjectMapper();

  private static final List<File> MANAGEMENT_PLUGINS = Stream.of(System.getProperty("managementPlugins", "").split(File.pathSeparator))
    .map(File::new)
    .collect(Collectors.toList());

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("build/cluster"), 1, MANAGEMENT_PLUGINS, "", RESOURCE_CONFIG, "");

  @BeforeClass
  public static void beforeClass() throws Exception {
    mapper.configure(SerializationFeature.INDENT_OUTPUT, true);

    CLUSTER.getClusterControl().waitForActive();

    consumer = new MonitoringServiceEntityFactory(ConnectionFactory.connect(CLUSTER.getConnectionURI(), new Properties())).retrieveOrCreate("MonitoringConsumerEntity");
    consumer.createMessageBuffer(1024);

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
    clientIdentifier = consumer.readTopology().getClients().values()
      .stream()
      .filter(client -> client.getName().equals("Ehcache:my-server-entity-1"))
      .findFirst()
      .map(Client::getClientIdentifier)
      .get();

    serverEntityIdentifier = consumer.readTopology()
      .activeServerEntityStream()
      .filter(serverEntity -> serverEntity.getName().equals("my-server-entity-1"))
      .findFirst()
      .get() // throws if not found
      .getServerEntityIdentifier();

    // test_notifs_sent_at_CM_init
    List<Message> messages = consumer.drainMessageBuffer();
    List<String> notificationTypes = notificationTypes(messages);
    assertThat(notificationTypes.get(0), equalTo("CLIENT_CONNECTED"));
    assertThat(notificationTypes.containsAll(Arrays.asList(
      "SERVER_ENTITY_CREATED", "SERVER_ENTITY_FETCHED",
      "ENTITY_REGISTRY_AVAILABLE", "ENTITY_REGISTRY_UPDATED", "EHCACHE_RESOURCE_POOLS_CONFIGURED", "EHCACHE_CLIENT_VALIDATED", "EHCACHE_SERVER_STORE_CREATED",
      "CLIENT_REGISTRY_AVAILABLE", "CLIENT_TAGS_UPDATED")), is(true));
    assertThat(consumer.readMessageBuffer(), is(nullValue()));
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
    if (consumer != null) {
      consumer.clearMessageBuffer();
    }
  }

  protected static ContextualReturn<?> sendManagementCallToCollectStats(String... statNames) throws Exception {
    Connection managementConnection = CLUSTER.newConnection();
    try {
      ManagementAgentService agent = new ManagementAgentService(new ManagementAgentEntityFactory(managementConnection).retrieveOrCreate(new ManagementAgentConfig()));

      final AtomicReference<String> managementCallId = new AtomicReference<>();
      final Exchanger<ContextualReturn<?>> exchanger = new Exchanger<>();

      agent.setContextualReturnListener((from, id, aReturn) -> {
        try {
          assertEquals(clientIdentifier, from);
          assertEquals(managementCallId.get(), id);
          exchanger.exchange(aReturn);
        } catch (InterruptedException e) {
          fail("interrupted");
        }
      });

      managementCallId.set(agent.call(
        clientIdentifier,
        Context.create("cacheManagerName", "my-super-cache-manager"),
        "StatisticCollectorCapability",
        "updateCollectedStatistics",
        Collection.class,
        new Parameter("StatisticsCapability"),
        new Parameter(asList(statNames), Collection.class.getName())));

      ContextualReturn<?> contextualReturn = exchanger.exchange(null);
      assertThat(contextualReturn.hasExecuted(), is(true));

      return contextualReturn;
    } finally {
      managementConnection.close();
    }
  }

  protected static List<ContextualStatistics> waitForNextStats() {
    // uses the monitoring consumre entity to get the content of the stat buffer when some stats are collected
    while (!Thread.currentThread().isInterrupted()) {
      List<ContextualStatistics> messages = consumer.drainMessageBuffer()
        .stream()
        .filter(message -> message.getType().equals("STATISTICS"))
        .flatMap(message -> message.unwrap(ContextualStatistics.class).stream())
        .collect(Collectors.toList());
      if(messages.isEmpty()) {
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

}
