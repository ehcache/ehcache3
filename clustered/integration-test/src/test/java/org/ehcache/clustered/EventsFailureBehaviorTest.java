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
package org.ehcache.clustered;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.reconnect.ThrowingResiliencyStrategy;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.stream.LongStream.range;
import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class EventsFailureBehaviorTest extends ClusteredTests {

  private static final int KEYS = 500;
  private static final org.awaitility.Duration TIMEOUT = org.awaitility.Duration.FIVE_SECONDS;

  private static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @Rule
  public Cluster CLUSTER =
    newCluster(2).in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();
  private PersistentCacheManager cacheManager1;
  private PersistentCacheManager cacheManager2;

  @Before
  public void waitForActive() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/event-cm"))
        .autoCreate()
        .defaultServerResource("primary-server-resource")).build(true);

    cacheManager2 = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve("/event-cm"))
        .autoCreate()
        .defaultServerResource("primary-server-resource")).build(true);
  }

  @After
  public void tearDown() {
    try {
      cacheManager1.close();
    } finally {
      cacheManager2.close();
    }
  }

  private static Cache<Long, byte[]> createCache(CacheManager cacheManager, CacheEventListener<?, ?> cacheEventListener, ExpiryPolicy<? super Long, ? super byte[]> expiryPolicy) {
    return cacheManager.createCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
      .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
      .add(CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(cacheEventListener, EnumSet.allOf(EventType.class))
        .unordered().asynchronous())
      .withExpiry(expiryPolicy)
      .build());
  }

  private void failover(Cache<Long, byte[]> cache1, Cache<Long, byte[]> cache2) throws Exception {
    // failover passive -> active
    CLUSTER.getClusterControl().terminateActive();
    CLUSTER.getClusterControl().waitForActive();

    // wait for clients to be back in business
    await().atMost(TIMEOUT).until(() -> {
      try {
        cache1.replace(1L, new byte[0], new byte[0]);
        cache2.replace(1L, new byte[0], new byte[0]);
        return true;
      } catch (Exception e) {
        return false;
      }
    });
  }

  @Test
  public void testEventsFailover() throws Exception {
    AccountingCacheEventListener<Long, byte[]> accountingCacheEventListener1 = new AccountingCacheEventListener<>();
    Cache<Long, byte[]> cache1 = createCache(cacheManager1, accountingCacheEventListener1, ExpiryPolicyBuilder.noExpiration());
    AccountingCacheEventListener<Long, byte[]> accountingCacheEventListener2 = new AccountingCacheEventListener<>();
    Cache<Long, byte[]> cache2 = createCache(cacheManager2, accountingCacheEventListener2, ExpiryPolicyBuilder.noExpiration());


    byte[] value = new byte[10 * 1024];

    range(0, KEYS).forEach(k -> {
      cache1.put(k, value);
    });
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.CREATED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.EVICTED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.CREATED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.EVICTED).size(), greaterThan(0));

    // failover passive -> active
    failover(cache1, cache2);

    range(0, KEYS).forEach(k -> {
      cache1.put(k, value);
    });
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.UPDATED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.UPDATED).size(), greaterThan(0));

    range(0, KEYS).forEach(cache1::remove);
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.REMOVED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.REMOVED).size(), greaterThan(0));

    range(KEYS, KEYS * 2).forEach(k -> {
      cache1.put(k, value);
    });
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.CREATED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.EVICTED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.CREATED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.EVICTED).size(), greaterThan(0));
  }

  @Test
  public void testExpirationFailover() throws Exception {
    AccountingCacheEventListener<Long, byte[]> accountingCacheEventListener1 = new AccountingCacheEventListener<>();
    Cache<Long, byte[]> cache1 = createCache(cacheManager1, accountingCacheEventListener1, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(1)));
    AccountingCacheEventListener<Long, byte[]> accountingCacheEventListener2 = new AccountingCacheEventListener<>();
    Cache<Long, byte[]> cache2 = createCache(cacheManager2, accountingCacheEventListener2, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(1)));


    byte[] value = new byte[10 * 1024];

    range(0, KEYS).forEach(k -> {
      cache1.put(k, value);
    });
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.CREATED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.EVICTED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.CREATED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.EVICTED).size(), greaterThan(0));

    // failover passive -> active
    failover(cache1, cache2);

    range(0, KEYS).forEach(k -> {
      assertThat(cache1.get(k), is(nullValue()));
    });
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener1.events.get(EventType.EXPIRED).size(), greaterThan(0));
    await().atMost(TIMEOUT).until(() -> accountingCacheEventListener2.events.get(EventType.EXPIRED).size(), greaterThan(0));
  }



  static class AccountingCacheEventListener<K, V> implements CacheEventListener<K, V> {
    private final Map<EventType, List<CacheEvent<? extends K, ? extends V>>> events;

    AccountingCacheEventListener() {
      events = new HashMap<>();
      clear();
    }

    @Override
    public void onEvent(CacheEvent<? extends K, ? extends V> event) {
      events.get(event.getType()).add(event);
    }

    final void clear() {
      for (EventType value : EventType.values()) {
        events.put(value, new CopyOnWriteArrayList<>());
      }
    }

  }
}
