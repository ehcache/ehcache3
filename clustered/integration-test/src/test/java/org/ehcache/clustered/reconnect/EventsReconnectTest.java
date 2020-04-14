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
package org.ehcache.clustered.reconnect;

import com.tc.net.proxy.TCPProxy;
import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.ClusteredTests;
import org.ehcache.clustered.TestRetryer;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.store.ReconnectInProgressException;
import org.ehcache.clustered.util.TCPProxyUtil;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventType;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static java.util.EnumSet.of;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.clustered.util.TCPProxyUtil.setDelay;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;
import static org.terracotta.utilities.test.WaitForAssert.assertThatEventually;

public class EventsReconnectTest extends ClusteredTests {
  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private static PersistentCacheManager cacheManager;

  private static class AccountingCacheEventListener<K, V> implements CacheEventListener<K, V> {
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

  private static AccountingCacheEventListener<Long, String> cacheEventListener = new AccountingCacheEventListener<>();

  private static CacheConfiguration<Long, String> config = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
    ResourcePoolsBuilder.newResourcePoolsBuilder()
      .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
    .withService(CacheEventListenerConfigurationBuilder
      .newEventListenerConfiguration(cacheEventListener, EnumSet.allOf(EventType.class))
      .unordered().asynchronous())
    .withResilienceStrategy(new ThrowingResiliencyStrategy<>())
    .build();

  private static final List<TCPProxy> proxies = new ArrayList<>();

  @ClassRule @Rule
  public static final TestRetryer<Integer, Cluster> CLUSTER = new TestRetryer<>(leaseLength -> newCluster()
    .in(clusterPath()).withServiceFragment(
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
        + "<ohr:offheap-resources>"
        + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
        + "</ohr:offheap-resources>"
        + "</config>\n"
        + "<service xmlns:lease='http://www.terracotta.org/service/lease'>"
        + "<lease:connection-leasing>"
        + "<lease:lease-length unit='seconds'>" + leaseLength + "</lease:lease-length>"
        + "</lease:connection-leasing>"
        + "</service>")
    .build(), of(TestRetryer.OutputIs.CLASS_RULE), 1, 10, 30);

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getOutput().getClusterControl().waitForActive();

    URI connectionURI = TCPProxyUtil.getProxyURI(CLUSTER.getOutput().getConnectionURI(), proxies);

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder
            = CacheManagerBuilder.newCacheManagerBuilder()
            .with(ClusteringServiceConfigurationBuilder.cluster(connectionURI.resolve("/crud-cm"))
                    .autoCreate(s -> s.defaultServerResource("primary-server-resource")));
    cacheManager = clusteredCacheManagerBuilder.build(false);
    cacheManager.init();
  }

  @Test
  public void eventsFlowAgainAfterReconnection() throws Exception {
    try {
      Cache<Long, String> cache = cacheManager.createCache("clustered-cache", config);

      CompletableFuture<Void> future = CompletableFuture.runAsync(() ->
              ThreadLocalRandom.current()
                      .longs()
                      .filter(val -> val != Long.MAX_VALUE)
                      .forEach(value ->
                              cache.put(value, Long.toString(value))));

      expireLease();

      try {
        future.get(5000, TimeUnit.MILLISECONDS);
        fail();
      } catch (ExecutionException e) {
        assertThat(e.getCause().getCause().getCause(), instanceOf(ReconnectInProgressException.class));
      }
      int beforeDisconnectionEventCounter = cacheEventListener.events.get(EventType.CREATED).size();

      CompletableFuture<Void> getSucceededFuture = CompletableFuture.runAsync(() -> {
        while (true) {
          try {
            cache.put(Long.MAX_VALUE, "");
            break;
          } catch (RuntimeException e) {

          }
        }
      });

      getSucceededFuture.get(20000, TimeUnit.MILLISECONDS);

      assertThatEventually(() -> cacheEventListener.events.get(EventType.CREATED), hasSize(beforeDisconnectionEventCounter + 1)).within(TIMEOUT);

    } finally {
      cacheManager.destroyCache("clustered-cache");
    }
  }


  private static void expireLease() throws InterruptedException {
    long delay = SECONDS.toMillis(CLUSTER.getInput() + 1);
    setDelay(delay, proxies);
    try {
      Thread.sleep(delay);
    } finally {
      setDelay(0L, proxies);
    }
  }
}
