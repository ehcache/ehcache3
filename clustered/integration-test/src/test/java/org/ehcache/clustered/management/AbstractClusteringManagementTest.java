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

import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.management.entity.management.ManagementAgentConfig;
import org.terracotta.management.entity.management.client.ContextualReturnListener;
import org.terracotta.management.entity.management.client.ManagementAgentEntityFactory;
import org.terracotta.management.entity.management.client.ManagementAgentService;
import org.terracotta.management.entity.monitoring.client.MonitoringServiceEntityFactory;
import org.terracotta.management.entity.monitoring.client.MonitoringServiceProxyEntity;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.call.Parameter;
import org.terracotta.management.model.cluster.ClientIdentifier;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.testing.rules.BasicExternalCluster;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public abstract class AbstractClusteringManagementTest {

  private static final String RESOURCE_CONFIG =
    "<service xmlns:ohr='http://www.terracotta.org/config/offheap-resource' id=\"resources\">"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</service>\n";

  protected static MonitoringServiceProxyEntity consumer;

  @ClassRule
  public static Cluster CLUSTER = new BasicExternalCluster(new File("build/cluster"), 1, getManagementPlugins(), "", RESOURCE_CONFIG, "");

  @BeforeClass
  public static void beforeClass() throws Exception {
    CLUSTER.getClusterControl().waitForActive();

    consumer = new MonitoringServiceEntityFactory(ConnectionFactory.connect(CLUSTER.getConnectionURI(), new Properties())).retrieveOrCreate("MonitoringConsumerEntity");
    consumer.createMessageBuffer(1024);
  }

  @After
  public final void clearBuffers() throws Exception {
    clear();
  }

  protected final void clear() {
    if(consumer != null) {
      consumer.clearMessageBuffer();
    }
  }

  protected static void sendManagementCallToCollectStats(String... statNames) throws Exception {
    Connection managementConnection = CLUSTER.newConnection();
    try {
      ManagementAgentService agent = new ManagementAgentService(new ManagementAgentEntityFactory(managementConnection).retrieveOrCreate(new ManagementAgentConfig()));

      assertThat(agent.getManageableClients().size(), equalTo(1)); // only ehcache client is manageable, not this one

      // find Ehcache client
      ClientIdentifier me = agent.getClientIdentifier();
      ClientIdentifier client = null;
      for (ClientIdentifier clientIdentifier : agent.getManageableClients()) {
        if (!clientIdentifier.equals(me)) {
          client = clientIdentifier;
          break;
        }
      }
      assertThat(client, is(notNullValue()));
      final ClientIdentifier ehcacheClientIdentifier = client;

      final CountDownLatch callCompleted = new CountDownLatch(1);
      final AtomicReference<String> managementCallId = new AtomicReference<String>();
      final BlockingQueue<ContextualReturn<?>> returns = new LinkedBlockingQueue<ContextualReturn<?>>();

      agent.setContextualReturnListener(new ContextualReturnListener() {
        @Override
        public void onContextualReturn(ClientIdentifier from, String id, ContextualReturn<?> aReturn) {
          try {
            Assert.assertEquals(ehcacheClientIdentifier, from);
            // make sure the call completed
            callCompleted.await(10, TimeUnit.SECONDS);
            assertEquals(managementCallId.get(), id);
            returns.offer(aReturn);
          } catch (InterruptedException e) {
            throw new RuntimeException(e);
          }
        }
      });

      managementCallId.set(agent.call(
        ehcacheClientIdentifier,
        Context.create("cacheManagerName", "my-super-cache-manager"),
        "StatisticCollectorCapability",
        "updateCollectedStatistics",
        Collection.class,
        new Parameter("StatisticsCapability"),
        new Parameter(asList(statNames), Collection.class.getName())));

      // now we're sure the call completed
      callCompleted.countDown();

      // ensure the call is made
      returns.take();
    } finally {
      managementConnection.close();
    }
  }

  protected static List<ContextualStatistics> waitForNextStats() {
    // uses the monitoring consumre entity to get the content of the stat buffer when some stats are collected
    while (true) {
      Message message = consumer.readMessageBuffer();
      if (message != null && message.getType().equals("STATISTICS")) {
        return message.unwrap(ContextualStatistics.class);
      }
      Thread.yield();
    }
  }

  private static List<File> getManagementPlugins() {
    String[] paths = System.getProperty("managementPlugins").split(File.pathSeparator);
    List<File> plugins = new ArrayList<File>(paths.length);
    for (String path : paths) {
      plugins.add(new File(path));
    }
    return plugins;
  }

  protected static List<String> messageTypes(List<Message> messages) {
    List<String> types = new ArrayList<String>(messages.size());
    for (Message message : messages) {
      types.add(message.getType());
    }
    return types;
  }

  protected static List<String> notificationTypes(List<Message> messages) {
    List<String> types = new ArrayList<String>(messages.size());
    for (Message message : messages) {
      if ("NOTIFICATION".equals(message.getType())) {
        for (ContextualNotification notification : message.unwrap(ContextualNotification.class)) {
          types.add(notification.getType());
        }
      }
    }
    return types;
  }

}
