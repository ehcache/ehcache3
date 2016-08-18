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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntityService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.EhcacheServerEntityService;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.entity.BasicServiceConfiguration;
import org.terracotta.entity.ServiceProviderConfiguration;
import org.terracotta.entity.map.TerracottaClusteredMapClientService;
import org.terracotta.entity.map.server.TerracottaClusteredMapService;
import org.terracotta.management.entity.ManagementAgentConfig;
import org.terracotta.management.entity.client.ContextualReturnListener;
import org.terracotta.management.entity.client.ManagementAgentEntityClientService;
import org.terracotta.management.entity.client.ManagementAgentEntityFactory;
import org.terracotta.management.entity.client.ManagementAgentService;
import org.terracotta.management.entity.server.ManagementAgentEntityServerService;
import org.terracotta.management.model.call.ContextualReturn;
import org.terracotta.management.model.call.Parameter;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.cluster.ClientIdentifier;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.primitive.Counter;
import org.terracotta.management.service.monitoring.IMonitoringConsumer;
import org.terracotta.management.service.monitoring.MonitoringServiceConfiguration;
import org.terracotta.management.service.monitoring.MonitoringServiceProvider;
import org.terracotta.management.service.monitoring.ReadOnlyBuffer;
import org.terracotta.offheapresource.OffHeapResourcesConfiguration;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughServer;

import java.io.Serializable;
import java.math.BigInteger;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class ClusteringManagementServiceTest {

  static IMonitoringConsumer consumer;
  static ReadOnlyBuffer<Serializable[]> notifsBuffer;
  static ReadOnlyBuffer<Serializable[]> statsBuffer;
  static PassthroughClusterControl stripeControl;

  CacheManager cacheManager;

  @Test //(timeout = 10000)
  public void test_programmatic_api() throws Exception {
    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        // cluster config
        .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("passthrough://server-1:9510/my-server-entity-1"))
            .autoCreate()
            .defaultServerResource("primary-server-resource"))
        // management config
        .using(new DefaultManagementRegistryConfiguration()
            .addTags("webapp-1", "server-node-1")
            .setCacheManagerAlias("my-super-cache-manager")
            .addConfiguration(new EhcacheStatisticsProviderConfiguration(
                1, TimeUnit.MINUTES,
                100, 1, TimeUnit.SECONDS,
                2, TimeUnit.SECONDS))) // TTD reduce to 2 seconds so that the stat collector run faster
        // cache config
        .withCache("cache-1", CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
            .build())
        .build(true);

    runTest();
  }

  @Test(timeout = 10000)
  public void test_xml_api() throws Exception {

    cacheManager = CacheManagerBuilder.newCacheManager(new XmlConfiguration(getClass().getResource("/ehcache-management-clustered.xml")));
    cacheManager.init();

    runTest();
  }

  private void runTest() throws Exception {
    // assert management registry has been correctly exposed in voltron
    String clientIdentifier = consumer.getChildNamesForNode("management", "clients").get().iterator().next();
    String[] tags = consumer.getValueForNode(new String[]{"management", "clients", clientIdentifier, "tags"}, String[].class).get();
    assertThat(tags, equalTo(new String[]{"server-node-1", "webapp-1"}));
    ContextContainer contextContainer = consumer.getValueForNode(new String[]{"management", "clients", clientIdentifier, "registry", "contextContainer"}, ContextContainer.class).get();
    assertThat(contextContainer.getValue(), equalTo("my-super-cache-manager"));
    assertThat(contextContainer.getSubContexts(), hasSize(1));
    assertThat(contextContainer.getSubContexts().iterator().next().getValue(), equalTo("cache-1"));
    Capability[] capabilities = consumer.getValueForNode(new String[]{"management", "clients", clientIdentifier, "registry", "capabilities"}, Capability[].class).get();
    assertThat(capabilities.length, equalTo(5));

    remotelyUpdateCollectedStatistics();

    // issue some puts to record some stats
    Cache<String, String> cache1 = cacheManager.getCache("cache-1", String.class, String.class);
    cache1.put("key1", "val");
    cache1.put("key2", "val");

    // create dynamically another cache to get the client-side notif
    cacheManager.createCache("cache-2", CacheConfigurationBuilder.newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB)
            .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
        .build());

    // assert that the management registry exposed in voltron has been updated
    contextContainer = consumer.getValueForNode(new String[]{"management", "clients", clientIdentifier, "registry", "contextContainer"}, ContextContainer.class).get();
    assertThat(contextContainer.getValue(), equalTo("my-super-cache-manager"));
    assertThat(contextContainer.getSubContexts(), hasSize(2));
    assertThat(contextContainer.getSubContexts().iterator().next().getValue(), equalTo("cache-1"));

    Collection<String> cNames = new TreeSet<String>();
    Collection<String> expectedCNames = new TreeSet<String>(Arrays.asList("cache-1", "cache-2"));
    for (ContextContainer container : contextContainer.getSubContexts()) {
      cNames.add(container.getValue());
    }
    assertThat(cNames, equalTo(expectedCNames));

    // verify the notification has been received in voltron
    ContextualNotification notif = (ContextualNotification) notifsBuffer.take()[1];
    assertThat(notif.getType(), equalTo("CACHE_ADDED"));

    // do some put also
    Cache<String, String> cache2 = cacheManager.getCache("cache-2", String.class, String.class);
    cache2.put("key1", "val");
    cache2.put("key2", "val");
    cache2.put("key3", "val");

    // verify stats have been received too
    ContextualStatistics[] stats = (ContextualStatistics[]) statsBuffer.take()[1];
    assertThat(stats.length, equalTo(2));
    assertThat(stats[0].getContext().get("cacheName"), equalTo("cache-1"));
    assertThat(stats[1].getContext().get("cacheName"), equalTo("cache-2"));
    assertThat(stats[0].getStatistic(Counter.class, "PutCounter").getValue(), equalTo(2L));
    assertThat(stats[1].getStatistic(Counter.class, "PutCounter").getValue(), equalTo(3L));

    cache1.put("key1", "val");
    cache1.put("key2", "val");
    cache2.put("key1", "val");
    cache2.put("key2", "val");
    cache2.put("key3", "val");

    // wait for next stats and verify
    stats = (ContextualStatistics[]) statsBuffer.take()[1];
    assertThat(stats.length, equalTo(2));
    assertThat(stats[0].getContext().get("cacheName"), equalTo("cache-1"));
    assertThat(stats[1].getContext().get("cacheName"), equalTo("cache-2"));
    assertThat(stats[0].getStatistic(Counter.class, "PutCounter").getValue(), equalTo(4L));
    assertThat(stats[1].getStatistic(Counter.class, "PutCounter").getValue(), equalTo(6L));

    // remove a cache
    cacheManager.removeCache("cache-2");

    // ensure we got the notification server-side
    notif = (ContextualNotification) notifsBuffer.read()[1];
    assertThat(notif.getType(), equalTo("CACHE_REMOVED"));
  }

  private void remotelyUpdateCollectedStatistics() throws Exception {
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
          new Parameter(asList("PutCounter", "InexistingRate"), Collection.class.getName())));

      // now we're sure the call completed
      callCompleted.countDown();

      // ensure the call is made
      returns.take();
    }
  }

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

    // clustered map (required by ehcache)
    activeServer.registerClientEntityService(new TerracottaClusteredMapClientService());
    activeServer.registerServerEntityService(new TerracottaClusteredMapService());

    // monitoring service
    activeServer.registerServiceProvider(new HackedMonitoringServiceProvider(), new MonitoringServiceConfiguration().setDebug(false));

    // off-heap service
    OffheapResourcesType offheapResourcesType = new OffheapResourcesType();
    ResourceType resourceType = new ResourceType();
    resourceType.setName("primary-server-resource");
    resourceType.setUnit(org.terracotta.offheapresource.config.MemoryUnit.MB);
    resourceType.setValue(BigInteger.TEN);
    offheapResourcesType.getResource().add(resourceType);
    activeServer.registerServiceProvider(new OffHeapResourcesProvider(), new OffHeapResourcesConfiguration(offheapResourcesType));

    stripeControl = new PassthroughClusterControl("server-1", activeServer);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    stripeControl.tearDown();
  }

  @After
  public void after() throws Exception {
    if (cacheManager != null && cacheManager.getStatus() == Status.AVAILABLE) {
      cacheManager.close();
    }
  }

  // hack to be able to access the IMonitoringConsumer interface outside of Voltron
  public static class HackedMonitoringServiceProvider extends MonitoringServiceProvider {
    @Override
    public boolean initialize(ServiceProviderConfiguration configuration) {
      super.initialize(configuration);
      consumer = getService(0, new BasicServiceConfiguration<>(IMonitoringConsumer.class));
      notifsBuffer = consumer.getOrCreateBestEffortBuffer("client-notifications", 1024, Serializable[].class);
      statsBuffer = consumer.getOrCreateBestEffortBuffer("client-statistics", 1024, Serializable[].class);
      return true;
    }
  }

}
