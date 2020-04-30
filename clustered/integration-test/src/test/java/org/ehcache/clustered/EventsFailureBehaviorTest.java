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
import org.ehcache.StateTransitionException;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.reconnect.ThrowingResiliencyStrategy;
import org.ehcache.clustered.util.ParallelTestCluster;
import org.ehcache.clustered.util.runners.Parallel;
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
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static java.util.stream.LongStream.range;
import static org.ehcache.clustered.client.config.builders.TimeoutsBuilder.timeouts;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;
import static org.terracotta.utilities.test.WaitForAssert.assertThatEventually;

@RunWith(Parallel.class)
public class EventsFailureBehaviorTest extends ClusteredTests {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventsFailureBehaviorTest.class);

  private static final int KEYS = 500;
  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final Duration FAILOVER_TIMEOUT = Duration.ofMinutes(1);

  private static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule @Rule
  public static final ParallelTestCluster CLUSTER = new ParallelTestCluster(newCluster(2).in(clusterPath()).withServiceFragment(RESOURCE_CONFIG).build());
  @Rule
  public final TestName testName = new TestName();

  private PersistentCacheManager cacheManager1;
  private PersistentCacheManager cacheManager2;

  @Before
  public void waitForActive() throws Exception {
    CLUSTER.getClusterControl().startAllServers();
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();

    cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve(testName.getMethodName()))
        .timeouts(timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(20)))
        .autoCreate(s -> s.defaultServerResource("primary-server-resource"))).build(true);

    cacheManager2 = CacheManagerBuilder.newCacheManagerBuilder()
      .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER.getConnectionURI().resolve(testName.getMethodName()))
        .timeouts(timeouts().read(Duration.ofSeconds(20)).write(Duration.ofSeconds(20)))
        .autoCreate(s -> s.defaultServerResource("primary-server-resource"))).build(true);
  }

  @After
  public void tearDown() {
    try {
      try {
        cacheManager1.close();
      } catch (StateTransitionException e) {
        LOGGER.warn("Failed to shutdown cache manager", e);
      }
    } finally {
      try {
        cacheManager2.close();
      } catch (StateTransitionException e) {
        LOGGER.warn("Failed to shutdown cache manager", e);
      }
    }
  }

  private static Cache<Long, byte[]> createCache(CacheManager cacheManager, CacheEventListener<?, ?> cacheEventListener, ExpiryPolicy<? super Long, ? super byte[]> expiryPolicy) {
    return cacheManager.createCache("cache", CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, byte[].class,
      ResourcePoolsBuilder.newResourcePoolsBuilder()
        .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 4, MemoryUnit.MB)))
      .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
      .withService(CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(cacheEventListener, EnumSet.allOf(EventType.class))
        .unordered().asynchronous())
      .withExpiry(expiryPolicy)
      .build());
  }

  private void failover(Cache<Long, byte[]> cache1, Cache<Long, byte[]> cache2) throws Exception {
    // failover passive -> active
    CLUSTER.getClusterControl().waitForRunningPassivesInStandby();
    CLUSTER.getClusterControl().terminateActive();

    // wait for clients to be back in business
    assertThatEventually(() -> {
      try {
        cache1.replace(1L, new byte[0], new byte[0]);
        cache2.replace(1L, new byte[0], new byte[0]);
        return true;
      } catch (Exception e) {
        return false;
      }
    }, is(true)).within(FAILOVER_TIMEOUT);
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
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.CREATED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.EVICTED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.CREATED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.EVICTED), hasSize(greaterThan(0))).within(TIMEOUT);

    // failover passive -> active
    failover(cache1, cache2);

    range(0, KEYS).forEach(k -> {
      cache1.put(k, value);
    });
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.UPDATED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.UPDATED), hasSize(greaterThan(0))).within(TIMEOUT);

    range(0, KEYS).forEach(cache1::remove);
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.REMOVED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.REMOVED), hasSize(greaterThan(0))).within(TIMEOUT);

    range(KEYS, KEYS * 2).forEach(k -> {
      cache1.put(k, value);
    });
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.CREATED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.EVICTED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.CREATED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.EVICTED), hasSize(greaterThan(0))).within(TIMEOUT);
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
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.CREATED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.EVICTED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.CREATED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.EVICTED), hasSize(greaterThan(0))).within(TIMEOUT);

    // failover passive -> active
    failover(cache1, cache2);

    range(0, KEYS).forEach(k -> {
      assertThat(cache1.get(k), is(nullValue()));
    });
    assertThatEventually(() -> accountingCacheEventListener1.events.get(EventType.EXPIRED), hasSize(greaterThan(0))).within(TIMEOUT);
    assertThatEventually(() -> accountingCacheEventListener2.events.get(EventType.EXPIRED), hasSize(greaterThan(0))).within(TIMEOUT);
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
