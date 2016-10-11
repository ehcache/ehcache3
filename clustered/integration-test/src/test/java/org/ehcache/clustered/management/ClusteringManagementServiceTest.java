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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.terracotta.management.entity.management.ManagementAgentConfig;
import org.terracotta.management.entity.management.client.ManagementAgentEntityFactory;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Sample;
import org.terracotta.management.model.stats.history.CounterHistory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.junit.Assert.assertThat;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.StatisticType;

public class ClusteringManagementServiceTest extends AbstractClusteringManagementTest {

  private static final Collection<Descriptor> ONHEAP_DESCRIPTORS = new ArrayList<Descriptor>();
  private static final Collection<Descriptor> OFFHEAP_DESCRIPTORS = new ArrayList<Descriptor>();
  private static final Collection<Descriptor> DISK_DESCRIPTORS =  new ArrayList<Descriptor>();
  private static final Collection<Descriptor> CLUSTERED_DESCRIPTORS =  new ArrayList<Descriptor>();
  private static final Collection<Descriptor> CACHE_DESCRIPTORS = new ArrayList<Descriptor>();

  private static AtomicInteger N = new AtomicInteger();

  @Rule
  public final Timeout globalTimeout = Timeout.seconds(60);

  private CacheManager cacheManager;
  private String clientIdentifier;
  private long consumerId;

  @Before
  public void init() throws Exception {
    this.cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      // cluster config
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/my-server-entity-" + N.incrementAndGet()))
        .autoCreate()
        .defaultServerResource("primary-server-resource"))
      // management config
      .using(new DefaultManagementRegistryConfiguration()
        .addTags("webapp-1", "server-node-1")
        .setCacheManagerAlias("my-super-cache-manager")
        .addConfiguration(new EhcacheStatisticsProviderConfiguration(
          1, TimeUnit.MINUTES,
          100, 1, TimeUnit.SECONDS,
          2, TimeUnit.SECONDS))) // TTD reduce to 2 seconds so that the stat collector runs faster
      // cache config
      .withCache("cache-1", CacheConfigurationBuilder.newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
        .build())
      .build(true);

    // ensure the CM is running and get its client id
    assertThat(cacheManager.getStatus(), equalTo(Status.AVAILABLE));
    consumerId = consumer.getConsumerId(ManagementAgentConfig.ENTITY_TYPE, ManagementAgentEntityFactory.ENTITYNAME);
    clientIdentifier = consumer.getChildNamesForNode(consumerId, "management", "clients").iterator().next();
  }

  @After
  public void close() throws Exception {
    if (cacheManager != null && cacheManager.getStatus() == Status.AVAILABLE) {
      cacheManager.close();
    }
  }

  @Test
  public void test_tags_exposed() throws Exception {
    String[] tags = consumer.getValueForNode(consumerId, new String[]{"management", "clients", clientIdentifier, "tags"}, String[].class);
    assertThat(tags, equalTo(new String[]{"server-node-1", "webapp-1"}));
  }

  @Test
  public void test_contextContainer_exposed() throws Exception {
    ContextContainer contextContainer = consumer.getValueForNode(consumerId, new String[]{"management", "clients", clientIdentifier, "registry", "contextContainer"}, ContextContainer.class);
    assertThat(contextContainer.getValue(), equalTo("my-super-cache-manager"));
    assertThat(contextContainer.getSubContexts(), hasSize(1));
    assertThat(contextContainer.getSubContexts().iterator().next().getValue(), equalTo("cache-1"));
  }

  @Test
  public void test_capabilities_exposed() throws Exception {
    Capability[] capabilities = consumer.getValueForNode(consumerId, new String[]{"management", "clients", clientIdentifier, "registry", "capabilities"}, Capability[].class);
    assertThat(capabilities.length, equalTo(5));
    assertThat(capabilities[0].getName(), equalTo("ActionsCapability"));
    assertThat(capabilities[1].getName(), equalTo("StatisticsCapability"));
    assertThat(capabilities[2].getName(), equalTo("StatisticCollectorCapability"));
    assertThat(capabilities[3].getName(), equalTo("SettingsCapability"));
    assertThat(capabilities[4].getName(), equalTo("ManagementAgentService"));
    assertThat(capabilities[0].getDescriptors(), hasSize(4));

    Collection<Descriptor> descriptors = capabilities[1].getDescriptors();
    Collection<Descriptor> allDescriptors = new ArrayList<Descriptor>();
    allDescriptors.addAll(CACHE_DESCRIPTORS);
    allDescriptors.addAll(ONHEAP_DESCRIPTORS);
    allDescriptors.addAll(OFFHEAP_DESCRIPTORS);
    allDescriptors.addAll(CLUSTERED_DESCRIPTORS);

    assertThat(descriptors, containsInAnyOrder(allDescriptors.toArray()));
    assertThat(descriptors, hasSize(allDescriptors.size()));

  }

  @Test
  public void test_notifs_sent_at_CM_init() throws Exception {
    assertThat(consumer.readBuffer("client-notifications", Message.class).unwrap(ContextualNotification.class).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(consumer.readBuffer("client-notifications", Message.class).unwrap(ContextualNotification.class).getType(), equalTo("CLIENT_TAGS_UPDATED"));
    assertThat(consumer.readBuffer("client-notifications", Serializable[].class), is(nullValue()));
  }

  @Test
  public void test_notifs_on_add_cache() throws Exception {
    clear();

    cacheManager.createCache("cache-2", CacheConfigurationBuilder.newCacheConfigurationBuilder(
      String.class, String.class,
      newResourcePoolsBuilder()
        .heap(10, EntryUnit.ENTRIES)
        .offheap(1, MemoryUnit.MB)
        .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
      .build());

    ContextContainer contextContainer = consumer.getValueForNode(consumerId, new String[]{"management", "clients", clientIdentifier, "registry", "contextContainer"}, ContextContainer.class);
    assertThat(contextContainer.getSubContexts(), hasSize(2));

    TreeSet<String> cNames = new TreeSet<String>();
    for (ContextContainer container : contextContainer.getSubContexts()) {
      cNames.add(container.getValue());
    }
    assertThat(cNames, equalTo(new TreeSet<String>(Arrays.asList("cache-1", "cache-2"))));

    assertThat(consumer.readBuffer("client-notifications", Message.class).unwrap(ContextualNotification.class).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(consumer.readBuffer("client-notifications", Message.class).unwrap(ContextualNotification.class).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(consumer.readBuffer("client-notifications", Message.class).unwrap(ContextualNotification.class).getType(), equalTo("CACHE_ADDED"));
    assertThat(consumer.readBuffer("client-notifications", Message.class), is(nullValue()));
  }

  @Test
  public void test_notifs_on_remove_cache() throws Exception {
    test_notifs_on_add_cache();

    cacheManager.removeCache("cache-2");

    assertThat(consumer.readBuffer("client-notifications", Message.class).unwrap(ContextualNotification.class).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(consumer.readBuffer("client-notifications", Message.class).unwrap(ContextualNotification.class).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(consumer.readBuffer("client-notifications", Message.class).unwrap(ContextualNotification.class).getType(), equalTo("CACHE_REMOVED"));
    assertThat(consumer.readBuffer("client-notifications", Message.class), is(nullValue()));
  }

  @Test
  public void test_stats_collection() throws Exception {

    sendManagementCallToCollectStats("Cache:HitCount");

    Cache<String, String> cache1 = cacheManager.getCache("cache-1", String.class, String.class);
    cache1.put("key1", "val");
    cache1.put("key2", "val");

    cache1.get("key1");
    cache1.get("key2");


    long val = 0;

    // it could be several seconds before the sampled stats could become available
    // let's try until we find the correct value : 2
    do {

      // get the stats (we are getting the primitive counter, not the sample history)
      ContextualStatistics[] stats = waitForNextStats();
      Sample<Long>[] samples = stats[0].getStatistic(CounterHistory.class, "Cache:HitCount").getValue();

      if(stats.length == 1 && stats[0].getContext().get("cacheName").equals("cache-1") && samples.length > 0) {
        val = samples[samples.length - 1].getValue();
      }
    } while(val != 2);

    // do some other operations
    cache1.get("key1");
    cache1.get("key2");

    do {

      ContextualStatistics[] stats = waitForNextStats();
      Sample<Long>[] samples = stats[0].getStatistic(CounterHistory.class, "Cache:HitCount").getValue();

      if(stats.length == 1 && stats[0].getContext().get("cacheName").equals("cache-1") && samples.length > 0) {
        val = samples[samples.length - 1].getValue();
      }

    } while(val != 4);


  }

  @BeforeClass
  public static void initDescriptors() throws ClassNotFoundException {
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissLatencyMinimum" , StatisticType.DURATION_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionLatencyMinimum" , StatisticType.DURATION_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissLatencyMaximum" , StatisticType.DURATION_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionRate" , StatisticType.RATE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitRatioRatio" , StatisticType.RATIO_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MappingCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitLatencyAverage" , StatisticType.AVERAGE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitLatencyMinimum" , StatisticType.DURATION_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:OccupiedBytesCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissRate" , StatisticType.RATE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionLatencyAverage" , StatisticType.AVERAGE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitCount" , StatisticType.COUNTER_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitRate" , StatisticType.RATE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissLatencyAverage" , StatisticType.AVERAGE_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionLatencyMaximum" , StatisticType.DURATION_HISTORY));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitLatencyMaximum" , StatisticType.DURATION_HISTORY));

    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionRate", StatisticType.RATE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissRate", StatisticType.RATE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitLatencyMinimum", StatisticType.DURATION_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitLatencyAverage", StatisticType.AVERAGE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:OccupiedBytesCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionLatencyAverage", StatisticType.AVERAGE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitLatencyMaximum", StatisticType.DURATION_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:AllocatedBytesCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionLatencyMaximum", StatisticType.DURATION_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MappingCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitRate", StatisticType.RATE_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitRatioRatio", StatisticType.RATIO_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionLatencyMinimum", StatisticType.DURATION_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissLatencyMinimum", StatisticType.DURATION_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissLatencyMaximum", StatisticType.DURATION_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MaxMappingCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitCount", StatisticType.COUNTER_HISTORY));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissLatencyAverage", StatisticType.AVERAGE_HISTORY));

    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissLatencyMaximum", StatisticType.DURATION_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissLatencyAverage", StatisticType.AVERAGE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitLatencyMinimum", StatisticType.DURATION_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MaxMappingCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitRate", StatisticType.RATE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:OccupiedBytesCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitLatencyAverage", StatisticType.AVERAGE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionLatencyAverage", StatisticType.AVERAGE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionLatencyMinimum", StatisticType.DURATION_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionRate", StatisticType.RATE_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionLatencyMaximum", StatisticType.DURATION_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:AllocatedBytesCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitLatencyMaximum", StatisticType.DURATION_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissLatencyMinimum", StatisticType.DURATION_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitRatioRatio", StatisticType.RATIO_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MappingCount", StatisticType.COUNTER_HISTORY));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissRate", StatisticType.RATE_HISTORY));

    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MissCount", StatisticType.COUNTER_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:EvictionLatencyMinimum", StatisticType.DURATION_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:HitCount", StatisticType.COUNTER_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MissLatencyMaximum", StatisticType.DURATION_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MaxMappingCount", StatisticType.COUNTER_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:HitRate", StatisticType.RATE_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:EvictionLatencyAverage", StatisticType.AVERAGE_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:EvictionCount", StatisticType.COUNTER_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:HitLatencyAverage", StatisticType.AVERAGE_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:HitLatencyMaximum", StatisticType.DURATION_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MissRate", StatisticType.RATE_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:OccupiedBytesCount", StatisticType.COUNTER_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MissLatencyMinimum", StatisticType.DURATION_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:EvictionLatencyMaximum", StatisticType.DURATION_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MissLatencyAverage", StatisticType.AVERAGE_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:HitRatioRatio", StatisticType.RATIO_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:AllocatedBytesCount", StatisticType.COUNTER_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MappingCount", StatisticType.COUNTER_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:EvictionRate", StatisticType.RATE_HISTORY));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:HitLatencyMinimum", StatisticType.DURATION_HISTORY));

    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissLatencyMaximum", StatisticType.DURATION_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitRate", StatisticType.RATE_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitLatencyMinimum", StatisticType.DURATION_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitCount", StatisticType.COUNTER_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitRatioRatio", StatisticType.RATIO_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissLatencyMinimum", StatisticType.DURATION_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:ClearLatencyAverage", StatisticType.AVERAGE_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitLatencyMaximum", StatisticType.DURATION_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:ClearRate", StatisticType.RATE_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissLatencyAverage", StatisticType.AVERAGE_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitLatencyAverage", StatisticType.AVERAGE_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:ClearLatencyMaximum", StatisticType.DURATION_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissRate", StatisticType.RATE_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:ClearCount", StatisticType.COUNTER_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:ClearLatencyMinimum", StatisticType.DURATION_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissCount", StatisticType.COUNTER_HISTORY));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissRatioRatio", StatisticType.RATIO_HISTORY));

  }

}
