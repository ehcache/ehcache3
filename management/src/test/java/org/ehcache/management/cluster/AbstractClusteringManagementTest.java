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
package org.ehcache.management.cluster;

import org.ehcache.clustered.client.internal.EhcacheClientEntityService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.EhcacheServerEntityService;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.management.entity.management.ManagementAgentConfig;
import org.terracotta.management.entity.management.client.ContextualReturnListener;
import org.terracotta.management.entity.management.client.ManagementAgentEntityClientService;
import org.terracotta.management.entity.management.client.ManagementAgentEntityFactory;
import org.terracotta.management.entity.management.client.ManagementAgentService;
import org.terracotta.management.entity.management.server.ManagementAgentEntityServerService;
import org.terracotta.management.entity.monitoring.client.MonitoringServiceEntity;
import org.terracotta.management.entity.monitoring.client.MonitoringServiceEntityClientService;
import org.terracotta.management.entity.monitoring.client.MonitoringServiceEntityFactory;
import org.terracotta.management.entity.monitoring.server.MonitoringServiceEntityServerService;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.call.Parameter;
import org.terracotta.management.model.cluster.ClientIdentifier;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.offheapresource.OffHeapResourcesConfiguration;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughServer;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.URI;
import java.util.Collection;
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

  protected static MonitoringServiceEntity consumer;

  private static PassthroughClusterControl stripeControl;

  @BeforeClass
  public static void beforeClass() throws Exception {
    PassthroughServer activeServer = new PassthroughServer();
    activeServer.setServerName("server-1");
    activeServer.setBindPort(9510);
    activeServer.setGroupPort(9610);

    // management agent entity
    activeServer.registerServerEntityService(new ManagementAgentEntityServerService());
    activeServer.registerClientEntityService(new ManagementAgentEntityClientService());

    // ehcache entity
    activeServer.registerServerEntityService(new EhcacheServerEntityService());
    activeServer.registerClientEntityService(new EhcacheClientEntityService());

    // RW lock entity (required by ehcache)
    activeServer.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
    activeServer.registerClientEntityService(new VoltronReadWriteLockEntityClientService());

    activeServer.registerServerEntityService(new MonitoringServiceEntityServerService());
    activeServer.registerClientEntityService(new MonitoringServiceEntityClientService());

    // off-heap service
    OffheapResourcesType offheapResourcesType = new OffheapResourcesType();
    ResourceType resourceType = new ResourceType();
    resourceType.setName("primary-server-resource");
    resourceType.setUnit(org.terracotta.offheapresource.config.MemoryUnit.MB);
    resourceType.setValue(BigInteger.TEN);
    offheapResourcesType.getResource().add(resourceType);
    activeServer.registerServiceProvider(new OffHeapResourcesProvider(), new OffHeapResourcesConfiguration(offheapResourcesType));

    stripeControl = new PassthroughClusterControl("server-1", activeServer);

    consumer = new MonitoringServiceEntityFactory(ConnectionFactory.connect(URI.create("passthrough://server-1:9510/cluster-1"), new Properties())).retrieveOrCreate("MonitoringConsumerEntity");
    consumer.createBestEffortBuffer("client-notifications", 1024, Serializable[].class);
    consumer.createBestEffortBuffer("client-statistics", 1024, Serializable[].class);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    if (stripeControl != null) {
      stripeControl.tearDown();
    }
  }

  @After
  public final void clearBuffers() throws Exception {
    clear();
  }

  protected final void clear() {
    while (consumer.readBuffer("client-notifications", Serializable[].class) != null) ;
    while (consumer.readBuffer("client-statistics", Serializable[].class) != null) ;
  }

  protected static void sendManagementCallToCollectStats(String... statNames) throws Exception {
    try (Connection managementConsole = ConnectionFactory.connect(URI.create("passthrough://server-1:9510/"), new Properties())) {
      ManagementAgentService agent = new ManagementAgentService(new ManagementAgentEntityFactory(managementConsole).retrieveOrCreate(new ManagementAgentConfig()));

      assertThat(agent.getManageableClients().size(), equalTo(2));

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

      CountDownLatch callCompleted = new CountDownLatch(1);
      AtomicReference<String> managementCallId = new AtomicReference<>();
      BlockingQueue<ContextualReturn<?>> returns = new LinkedBlockingQueue<>();

      agent.setContextualReturnListener(new ContextualReturnListener() {
        @Override
        public void onContextualReturn(ClientIdentifier from, String id, ContextualReturn<?> aReturn) {
          try {
            assertEquals(ehcacheClientIdentifier, from);
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
    }
  }

  protected static ContextualStatistics[] waitForNextStats() {
    // uses the monitoring consumre entity to get the content of the stat buffer when some stats are collected
    Serializable[] serializables;
    while ((serializables = consumer.readBuffer("client-statistics", Serializable[].class)) == null) { Thread.yield(); }
    return (ContextualStatistics[]) serializables[1];
  }

}
