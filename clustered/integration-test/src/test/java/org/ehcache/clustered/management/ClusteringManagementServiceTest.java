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
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runners.MethodSorters;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.cluster.Cluster;
import org.terracotta.management.model.cluster.ServerEntityIdentifier;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.message.Message;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class ClusteringManagementServiceTest extends AbstractClusteringManagementTest {

  private static final Collection<StatisticDescriptor> ONHEAP_DESCRIPTORS = new ArrayList<>();
  private static final Collection<StatisticDescriptor> OFFHEAP_DESCRIPTORS = new ArrayList<>();
  private static final Collection<StatisticDescriptor> DISK_DESCRIPTORS =  new ArrayList<>();
  private static final Collection<StatisticDescriptor> CLUSTERED_DESCRIPTORS =  new ArrayList<>();
  private static final Collection<StatisticDescriptor> CACHE_DESCRIPTORS = new ArrayList<>();
  private static final Collection<StatisticDescriptor> POOL_DESCRIPTORS = new ArrayList<>();
  private static final Collection<StatisticDescriptor> SERVER_STORE_DESCRIPTORS = new ArrayList<>();
  private static final Collection<StatisticDescriptor> OFFHEAP_RES_DESCRIPTORS = new ArrayList<>();

  @Test
  @Ignore("This is not a test, but something useful to show a json print of a cluster topology with all management metadata inside")
  public void test_A_topology() throws Exception {
    Cluster cluster = nmsService.readTopology();
    String json = mapper.writeValueAsString(cluster.toMap());
    //System.out.println(json);
  }

  @Test
  public void test_A_client_tags_exposed() throws Exception {
    String[] tags = readTopology().getClient(ehcacheClientIdentifier).get().getTags().toArray(new String[0]);
    assertThat(tags).containsOnly("server-node-1", "webapp-1");
  }

  @Test
  public void test_B_client_contextContainer_exposed() throws Exception {
    ContextContainer contextContainer = readTopology().getClient(ehcacheClientIdentifier).get().getManagementRegistry().get().getContextContainer();
    assertThat(contextContainer.getValue()).isEqualTo("my-super-cache-manager");
    Collection<ContextContainer> subContexts = contextContainer.getSubContexts();
    TreeSet<String> cacheNames = subContexts.stream().map(ContextContainer::getValue).collect(Collectors.toCollection(TreeSet::new));
    assertThat(cacheNames).isEqualTo(new TreeSet<>(Arrays.asList("dedicated-cache-1", "shared-cache-2", "shared-cache-3")));
  }

  @Test
  public void test_C_client_capabilities_exposed() throws Exception {
    Capability[] capabilities = readTopology().getClient(ehcacheClientIdentifier).get().getManagementRegistry().get().getCapabilities().toArray(new Capability[0]);
    assertThat(capabilities.length).isEqualTo(5);
    assertThat(capabilities[0].getName()).isEqualTo("ActionsCapability");
    assertThat(capabilities[1].getName()).isEqualTo("NmsAgentService");
    assertThat(capabilities[2].getName()).isEqualTo("SettingsCapability");
    assertThat(capabilities[3].getName()).isEqualTo("StatisticCollectorCapability");
    assertThat(capabilities[4].getName()).isEqualTo("StatisticsCapability");

    assertThat(capabilities[0].getDescriptors()).hasSize(4);

    Collection<? extends Descriptor> descriptors = capabilities[4].getDescriptors();
    Collection<Descriptor> allDescriptors = new ArrayList<>();
    allDescriptors.addAll(CACHE_DESCRIPTORS);
    allDescriptors.addAll(ONHEAP_DESCRIPTORS);
    allDescriptors.addAll(OFFHEAP_DESCRIPTORS);
    allDescriptors.addAll(CLUSTERED_DESCRIPTORS);

    assertThat(descriptors).containsOnlyElementsOf(allDescriptors);
  }

  @Test
  public void test_D_server_capabilities_exposed() throws Exception {
    Capability[] managerCapabilities = readTopology().getSingleStripe().getActiveServerEntity(clusterTierManagerEntityIdentifier).get().getManagementRegistry().get().getCapabilities().toArray(new Capability[0]);

    assertThat(managerCapabilities).hasSize(4);

    assertThat(managerCapabilities[0].getName()).isEqualTo("ClientStateSettings");
    assertThat(managerCapabilities[1].getName()).isEqualTo("ClusterTierManagerSettings");
    assertThat(managerCapabilities[2].getName()).isEqualTo("PoolSettings");
    assertThat(managerCapabilities[3].getName()).isEqualTo("PoolStatistics");

    ServerEntityIdentifier ehcacheClusterTierIdentifier = readTopology()
      .activeServerEntityStream()
      .filter(serverEntity -> serverEntity.getName().equals("my-server-entity-1$dedicated-cache-1"))
      .findFirst()
      .get() // throws if not found
      .getServerEntityIdentifier();

    Capability[] tierCapabilities = readTopology().getSingleStripe().getActiveServerEntity(ehcacheClusterTierIdentifier).get().getManagementRegistry().get().getCapabilities().toArray(new Capability[0]);
    assertThat(tierCapabilities).hasSize(5);

    assertThat(tierCapabilities[0].getName()).isEqualTo("ClusterTierClientStateSettings");
    assertThat(tierCapabilities[1].getName()).isEqualTo("PoolSettings");
    assertThat(tierCapabilities[2].getName()).isEqualTo("PoolStatistics");
    assertThat(tierCapabilities[3].getName()).isEqualTo("ServerStoreSettings");
    assertThat(tierCapabilities[4].getName()).isEqualTo("ServerStoreStatistics");

    // stats

    assertThat(tierCapabilities[4].getDescriptors()).containsOnlyElementsOf(SERVER_STORE_DESCRIPTORS);
    assertThat(managerCapabilities[3].getDescriptors()).containsOnlyElementsOf(POOL_DESCRIPTORS);
    assertThat(tierCapabilities[2].getDescriptors()).containsOnlyElementsOf(POOL_DESCRIPTORS);

    // ClientStateSettings

    assertThat(managerCapabilities[0].getDescriptors()).hasSize(1);
    Settings settings = (Settings) managerCapabilities[0].getDescriptors().iterator().next();
    assertThat((boolean) settings.get("attached")).isTrue();

    // ClusterTierManagerStateSettings

    assertThat(managerCapabilities[1].getDescriptors()).hasSize(1);
    settings = (Settings) managerCapabilities[1].getDescriptors().iterator().next();
    assertThat(settings.get("type")).isEqualTo("ClusterTierManager");
    assertThat(settings.get("defaultServerResource")).isEqualTo("primary-server-resource");

    // Shared PoolSettings

    List<Descriptor> descriptors = new ArrayList<>(managerCapabilities[2].getDescriptors());
    assertThat(descriptors).hasSize(2);

    settings = (Settings) descriptors.get(0);
    assertThat(settings.get("alias")).isEqualTo("resource-pool-b");
    assertThat(settings.get("type")).isEqualTo("Pool");
    assertThat(settings.get("serverResource")).isEqualTo("primary-server-resource");
    assertThat(settings.get("size")).isEqualTo(16 * 1024 * 1024L);
    assertThat(settings.get("allocationType")).isEqualTo("shared");

    settings = (Settings) descriptors.get(1);
    assertThat(settings.get("alias")).isEqualTo("resource-pool-a");
    assertThat(settings.get("type")).isEqualTo("Pool");
    assertThat(settings.get("serverResource")).isEqualTo("secondary-server-resource");
    assertThat(settings.get("size")).isEqualTo(28 * 1024 * 1024L);
    assertThat(settings.get("allocationType")).isEqualTo("shared");

    // ClusterTierClientStateSettings

    assertThat(tierCapabilities[0].getDescriptors()).hasSize(1);
    settings = (Settings) tierCapabilities[0].getDescriptors().iterator().next();
    assertThat((boolean) settings.get("attached")).isTrue();
    assertThat((String) settings.get("store")).isEqualTo("dedicated-cache-1");

    // Dedicated PoolSettings

    List<Descriptor> tierDescriptors = new ArrayList<>(tierCapabilities[1].getDescriptors());
    assertThat(tierDescriptors).hasSize(1);

    settings = (Settings) tierDescriptors.get(0);
    assertThat(settings.get("alias")).isEqualTo("dedicated-cache-1");
    assertThat(settings.get("type")).isEqualTo("Pool");
    assertThat(settings.get("serverResource")).isEqualTo("primary-server-resource");
    assertThat(settings.get("size")).isEqualTo(4 * 1024 * 1024L);
    assertThat(settings.get("allocationType")).isEqualTo("dedicated");

    // ServerStoreSettings

    tierDescriptors = new ArrayList<>(tierCapabilities[3].getDescriptors());
    assertThat(tierDescriptors).hasSize(2);

    settings = (Settings) tierDescriptors.get(0);
    assertThat(settings.get("alias")).isEqualTo("dedicated-cache-1");
    assertThat(settings.get("type")).isEqualTo("ServerStore");
    assertThat(settings.get("resourcePoolType")).isEqualTo("dedicated");
    assertThat(settings.get("resourcePoolDedicatedResourceName")).isEqualTo("primary-server-resource");

    settings = (Settings) tierDescriptors.get(1);
    assertThat(settings.get("type")).isEqualTo("ServerStoreSettings");
    assertThat(settings.get("clusterTierManager")).isEqualTo("my-server-entity-1");

    // tms entity

    managerCapabilities = readTopology().activeServerEntityStream().filter(serverEntity -> serverEntity.is(tmsServerEntityIdentifier)).findFirst().get().getManagementRegistry().get().getCapabilities().toArray(new Capability[0]);
    assertThat(managerCapabilities.length).isEqualTo(3);

    assertThat(managerCapabilities[0].getName()).isEqualTo("OffHeapResourceSettings");
    assertThat(managerCapabilities[1].getName()).isEqualTo("OffHeapResourceStatistics");
    assertThat(managerCapabilities[2].getName()).isEqualTo("StatisticCollectorCapability");

    assertThat(managerCapabilities[0].getDescriptors()).hasSize(3); // time + 2 resources

    assertThat(managerCapabilities[1].getDescriptors()).containsOnlyElementsOf(OFFHEAP_RES_DESCRIPTORS);
  }

  @Test
  public void test_E_notifs_on_add_cache() throws Exception {
    cacheManager.createCache("cache-2", newCacheConfigurationBuilder(
      String.class, String.class,
      newResourcePoolsBuilder()
        .heap(10, EntryUnit.ENTRIES)
        .offheap(1, MemoryUnit.MB)
        .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
      .build());

    ContextContainer contextContainer = readTopology().getClient(ehcacheClientIdentifier).get().getManagementRegistry().get().getContextContainer();
    assertThat(contextContainer.getSubContexts()).hasSize(4);

    TreeSet<String> cNames = contextContainer.getSubContexts().stream().map(ContextContainer::getValue).collect(Collectors.toCollection(TreeSet::new));
    assertThat(cNames).isEqualTo(new TreeSet<>(Arrays.asList("cache-2", "dedicated-cache-1", "shared-cache-2", "shared-cache-3")));

    List<Message> messages = readMessages();
    assertThat(notificationTypes(messages)).containsOnly("SERVER_ENTITY_CREATED", "ENTITY_REGISTRY_AVAILABLE", "EHCACHE_SERVER_STORE_CREATED", "SERVER_ENTITY_FETCHED", "EHCACHE_SERVER_STORE_ATTACHED", "CACHE_ADDED");
  }

  @Test
  public void test_F_notifs_on_remove_cache() throws Exception {
    cacheManager.removeCache("cache-2");

    List<Message> messages = readMessages();
    assertThat(notificationTypes(messages)).containsOnly("CACHE_REMOVED", "EHCACHE_SERVER_STORE_RELEASED", "SERVER_ENTITY_UNFETCHED");
  }

  @Test
  public void test_G_stats_collection() throws Exception {

    sendManagementCallOnClientToCollectStats();

    Cache<String, String> cache1 = cacheManager.getCache("dedicated-cache-1", String.class, String.class);
    cache1.put("key1", "val");
    cache1.put("key2", "val");

    cache1.get("key1"); // hit
    cache1.get("key2"); // hit

    List<ContextualStatistics> allStats = new ArrayList<>();
    long val = 0;

    // it could be several seconds before the sampled stats could become available
    // let's try until we find the correct value : 2
    do {

      // get the stats (we are getting the primitive counter, not the sample history)
      List<ContextualStatistics> stats = waitForNextStats();
      allStats.addAll(stats);

      // only keep CM stats for the following checks
      stats = stats.stream()
        .filter(statistics -> "dedicated-cache-1".equals(statistics.getContext().get("cacheName")))
        .collect(Collectors.toList());

      for (ContextualStatistics stat : stats) {
        val = stat.getStatistic("Cache:HitCount").longValue();
      }
    } while(!Thread.currentThread().isInterrupted() && val != 2);

    // do some other operations
    cache1.get("key1");
    cache1.get("key2");

    do {

      List<ContextualStatistics> stats = waitForNextStats();
      allStats.addAll(stats);
      // only keep CM stats for the following checks
      stats = stats.stream()
        .filter(statistics -> "dedicated-cache-1".equals(statistics.getContext().get("cacheName")))
        .collect(Collectors.toList());

      for (ContextualStatistics stat : stats) {
        val = stat.getStatistic("Cache:HitCount").longValue();
      }

    } while(!Thread.currentThread().isInterrupted() && val != 4);

    // wait until we have some stats coming from the server entity
    while (!Thread.currentThread().isInterrupted() &&  !allStats.stream().filter(statistics -> statistics.getContext().contains("consumerId")).findFirst().isPresent()) {
      allStats.addAll(waitForNextStats());
    }
    List<ContextualStatistics> serverStats = allStats.stream().filter(statistics -> statistics.getContext().contains("consumerId")).collect(Collectors.toList());

    // server-side stats
    TreeSet<String> capabilities = serverStats.stream()
      .map(ContextualStatistics::getCapability)
      .collect(Collectors.toCollection(TreeSet::new));

    assertThat(capabilities).containsOnly("PoolStatistics", "ServerStoreStatistics", "OffHeapResourceStatistics");

    // ensure we collect stats from all registered objects (pools and stores)

    Set<String> poolStats = serverStats.stream()
      .filter(statistics -> statistics.getCapability().equals("PoolStatistics"))
      .map(statistics -> statistics.getContext().get("alias"))
      .collect(Collectors.toSet());
    assertThat(poolStats).containsOnly("resource-pool-b", "resource-pool-a", "dedicated-cache-1", "cache-2");

    Set<String> serverStoreStats = serverStats.stream()
      .filter(statistics -> statistics.getCapability().equals("ServerStoreStatistics"))
      .map(statistics -> statistics.getContext().get("alias"))
      .collect(Collectors.toSet());
    assertThat(serverStoreStats).containsOnly("shared-cache-3", "shared-cache-2", "dedicated-cache-1", "cache-2");

    Set<String> offheapResourceStats = serverStats.stream()
      .filter(statistics -> statistics.getCapability().equals("OffHeapResourceStatistics"))
      .map(statistics -> statistics.getContext().get("alias"))
      .collect(Collectors.toSet());
    assertThat(offheapResourceStats).containsOnly("primary-server-resource", "secondary-server-resource");

    // ensure we collect all the stat names

    Set<String> poolDescriptors = serverStats.stream()
      .filter(statistics -> statistics.getCapability().equals("PoolStatistics"))
      .flatMap(statistics -> statistics.getStatistics().keySet().stream())
      .collect(Collectors.toSet());
    assertThat(poolDescriptors).containsOnlyElementsOf(POOL_DESCRIPTORS.stream().map(StatisticDescriptor::getName).collect(Collectors.toSet()));

    Set<String> serverStoreDescriptors = serverStats.stream()
      .filter(statistics -> statistics.getCapability().equals("ServerStoreStatistics"))
      .flatMap(statistics -> statistics.getStatistics().keySet().stream())
      .collect(Collectors.toSet());
    assertThat(serverStoreDescriptors).containsOnlyElementsOf(SERVER_STORE_DESCRIPTORS.stream().map(StatisticDescriptor::getName).collect(Collectors.toSet()));

    Set<String> offHeapResourceDescriptors = serverStats.stream()
      .filter(statistics -> statistics.getCapability().equals("OffHeapResourceStatistics"))
      .flatMap(statistics -> statistics.getStatistics().keySet().stream())
      .collect(Collectors.toSet());
    assertThat(offHeapResourceDescriptors).isEqualTo(OFFHEAP_RES_DESCRIPTORS.stream().map(StatisticDescriptor::getName).collect(Collectors.toSet()));
  }

  @BeforeClass
  public static void initDescriptors() throws ClassNotFoundException {
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:EvictionCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:ExpirationCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MissCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:MappingCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:OccupiedByteSize", "SIZE"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:HitCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:PutCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:UpdateCount" , "COUNTER"));
    ONHEAP_DESCRIPTORS.add(new StatisticDescriptor("OnHeap:RemovalCount" , "COUNTER"));

    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MissCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:OccupiedByteSize", "SIZE"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:AllocatedByteSize", "SIZE"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MappingCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:EvictionCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:ExpirationCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:MaxMappingCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:HitCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:PutCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:UpdateCount", "COUNTER"));
    OFFHEAP_DESCRIPTORS.add(new StatisticDescriptor("OffHeap:RemovalCount", "COUNTER"));

    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MaxMappingCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:OccupiedByteSize", "SIZE"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:AllocatedByteSize", "SIZE"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:HitCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:EvictionCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:ExpirationCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MissCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:MappingCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:PutCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:UpdateCount", "COUNTER"));
    DISK_DESCRIPTORS.add(new StatisticDescriptor("Disk:RemovalCount", "COUNTER"));

    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MissCount", "COUNTER"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:HitCount", "COUNTER"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:PutCount", "COUNTER"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:UpdateCount", "COUNTER"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:RemovalCount", "COUNTER"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MaxMappingCount", "COUNTER"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:EvictionCount", "COUNTER"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:ExpirationCount", "COUNTER"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:OccupiedByteSize", "SIZE"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:AllocatedByteSize", "SIZE"));
    CLUSTERED_DESCRIPTORS.add(new StatisticDescriptor("Clustered:MappingCount", "COUNTER"));

    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:HitCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:MissCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:PutCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:UpdateCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:RemovalCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:EvictionCount", "COUNTER"));
    CACHE_DESCRIPTORS.add(new StatisticDescriptor("Cache:ExpirationCount", "COUNTER"));

    POOL_DESCRIPTORS.add(new StatisticDescriptor("Pool:AllocatedSize", "SIZE"));

    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:AllocatedMemory", "SIZE"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:DataAllocatedMemory", "SIZE"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:OccupiedMemory", "SIZE"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:DataOccupiedMemory", "SIZE"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:Entries", "COUNTER"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:UsedSlotCount", "COUNTER"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:DataVitalMemory", "SIZE"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:VitalMemory", "SIZE"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:RemovedSlotCount", "COUNTER"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:DataSize", "SIZE"));
    SERVER_STORE_DESCRIPTORS.add(new StatisticDescriptor("Store:TableCapacity", "SIZE"));

    OFFHEAP_RES_DESCRIPTORS.add(new StatisticDescriptor("OffHeapResource:AllocatedMemory", "SIZE"));
  }

}
