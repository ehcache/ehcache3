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
import org.ehcache.ValueSupplier;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.management.entity.management.ManagementAgentConfig;
import org.terracotta.management.entity.management.client.ManagementAgentEntityFactory;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.primitive.Counter;

import java.io.Serializable;
import java.net.URI;
import java.util.Arrays;
import java.util.Collection;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class ClusteringManagementServiceTest extends AbstractClusteringManagementTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
      {
        new ValueSupplier<CacheManager>() {
          @Override
          public CacheManager value() {
            return CacheManagerBuilder.newCacheManagerBuilder()
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
                  2, TimeUnit.SECONDS))) // TTD reduce to 2 seconds so that the stat collector runs faster
              // cache config
              .withCache("cache-1", CacheConfigurationBuilder.newCacheConfigurationBuilder(
                String.class, String.class,
                newResourcePoolsBuilder()
                  .heap(10, EntryUnit.ENTRIES)
                  .offheap(1, MemoryUnit.MB)
                  .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
                .build())
              .build(true);
          }
        }
      }, {
      new ValueSupplier<CacheManager>() {
        @Override
        public CacheManager value() {
          CacheManager cacheManager = CacheManagerBuilder.newCacheManager(new XmlConfiguration(getClass().getResource("/ehcache-management-clustered.xml")));
          cacheManager.init();
          return cacheManager;
        }
      }
    }});
  }

  @Rule
  public final Timeout globalTimeout = new Timeout(10000);

  private final ValueSupplier<CacheManager> cacheManagerValueSupplier;

  private CacheManager cacheManager;
  private String clientIdentifier;
  private long consumerId;

  public ClusteringManagementServiceTest(ValueSupplier<CacheManager> cacheManagerValueSupplier) {
    this.cacheManagerValueSupplier = cacheManagerValueSupplier;
  }

  @Before
  public void init() throws Exception {
    this.cacheManager = cacheManagerValueSupplier.value();

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
    assertThat(capabilities[1].getDescriptors(), hasSize(13));
  }

  @Test
  public void test_notifs_sent_at_CM_init() throws Exception {
    assertThat(((ContextualNotification) consumer.readBuffer("client-notifications", Serializable[].class)[1]).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(((ContextualNotification) consumer.readBuffer("client-notifications", Serializable[].class)[1]).getType(), equalTo("CLIENT_TAGS_UPDATED"));
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
        .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
      .build());

    ContextContainer contextContainer = consumer.getValueForNode(consumerId, new String[]{"management", "clients", clientIdentifier, "registry", "contextContainer"}, ContextContainer.class);
    assertThat(contextContainer.getSubContexts(), hasSize(2));

    Collection<String> cNames = new TreeSet<String>();
    for (ContextContainer container : contextContainer.getSubContexts()) {
      cNames.add(container.getValue());
    }
    assertThat(cNames, equalTo(new TreeSet<String>(Arrays.asList("cache-1", "cache-2"))));

    assertThat(((ContextualNotification) consumer.readBuffer("client-notifications", Serializable[].class)[1]).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(((ContextualNotification) consumer.readBuffer("client-notifications", Serializable[].class)[1]).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(((ContextualNotification) consumer.readBuffer("client-notifications", Serializable[].class)[1]).getType(), equalTo("CACHE_ADDED"));
    assertThat(consumer.readBuffer("client-notifications", Serializable[].class), is(nullValue()));
  }

  @Test
  public void test_notifs_on_remove_cache() throws Exception {
    test_notifs_on_add_cache();

    cacheManager.removeCache("cache-2");

    assertThat(((ContextualNotification) consumer.readBuffer("client-notifications", Serializable[].class)[1]).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(((ContextualNotification) consumer.readBuffer("client-notifications", Serializable[].class)[1]).getType(), equalTo("CLIENT_REGISTRY_UPDATED"));
    assertThat(((ContextualNotification) consumer.readBuffer("client-notifications", Serializable[].class)[1]).getType(), equalTo("CACHE_REMOVED"));
    assertThat(consumer.readBuffer("client-notifications", Serializable[].class), is(nullValue()));
  }

  @Test
  public void test_stats_collection() throws Exception {

    sendManagementCallToCollectStats("GetCounter", "InexistingRate", "AllCacheGetCount");

    Cache<String, String> cache1 = cacheManager.getCache("cache-1", String.class, String.class);
    cache1.put("key1", "val");
    cache1.put("key2", "val");

    cache1.get("key1");
    cache1.get("key2");

    // get the stats (we are getting the primitive counter, not the sample history)
    ContextualStatistics[] stats = waitForNextStats();

    assertThat(stats.length, equalTo(1));
    assertThat(stats[0].getContext().get("cacheName"), equalTo("cache-1"));
    assertThat(stats[0].getStatistic(Counter.class, "GetCounter").getValue(), equalTo(2L));

    // first collect of a sample gives no value because it "triggers" the stat computation
    // this is how the internal ehcache's stat framework works: first call to a sample activates it.
    assertThat(stats[0].getContext().get("cacheName"), equalTo("cache-1"));
    assertThat(stats[0].getStatistic(CounterHistory.class, "AllCacheGetCount").getValue().length, equalTo(0));

    // do some other operations
    cache1.get("key1");
    cache1.get("key2");

    stats = waitForNextStats();

    assertThat(stats.length, equalTo(1));
    assertThat(stats[0].getContext().get("cacheName"), equalTo("cache-1"));
    assertThat(stats[0].getStatistic(Counter.class, "GetCounter").getValue(), equalTo(4L));
    assertThat(stats[0].getContext().get("cacheName"), equalTo("cache-1"));
    assertThat(stats[0].getStatistic(CounterHistory.class, "AllCacheGetCount").getValue().length, equalTo(1));
    assertThat(stats[0].getStatistic(CounterHistory.class, "AllCacheGetCount").getValue()[0].getValue(), equalTo(4L));
  }

}
