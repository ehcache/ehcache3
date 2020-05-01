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

package org.ehcache.clustered.client;

import org.ehcache.Cache;
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.ehcache.impl.internal.TimeSourceConfiguration;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static java.util.EnumSet.allOf;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ExpiryPolicyBuilder.timeToLiveExpiration;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;
import static org.terracotta.utilities.test.matchers.Eventually.within;

public class ClusteredEventsTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");

  @Rule
  public final TestName runningTest = new TestName();

  @Before
  public void definePassthroughServer() {
    UnitTestConnectionService.add(CLUSTER_URI,
      new UnitTestConnectionService.PassthroughServerBuilder()
        .resource("primary-server-resource", 32, MemoryUnit.MB)
        .build());
  }

  @After
  public void removePassthroughServer() {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void testNonExpiringEventSequence() throws TimeoutException {
    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
      newCacheManagerBuilder()
        .with(cluster(CLUSTER_URI).autoCreate(s -> s.defaultServerResource("primary-server-resource")))
        .withCache(runningTest.getMethodName(), newCacheConfigurationBuilder(Long.class, String.class,
          newResourcePoolsBuilder().with(clusteredDedicated(16, MemoryUnit.MB))));

    try (PersistentCacheManager driver = clusteredCacheManagerBuilder.build(true)) {
      Cache<Long, String> driverCache = driver.getCache(runningTest.getMethodName(), Long.class, String.class);
      try (PersistentCacheManager observer = clusteredCacheManagerBuilder.build(true)) {
        Cache<Long, String> observerCache = observer.getCache(runningTest.getMethodName(), Long.class, String.class);

        List<CacheEvent<? extends Long, ? extends String>> driverEvents = new ArrayList<>();
        driverCache.getRuntimeConfiguration().registerCacheEventListener(driverEvents::add, EventOrdering.ORDERED, EventFiring.ASYNCHRONOUS, allOf(EventType.class));

        List<CacheEvent<? extends Long, ? extends String>> observerEvents = new ArrayList<>();
        observerCache.getRuntimeConfiguration().registerCacheEventListener(observerEvents::add, EventOrdering.ORDERED, EventFiring.ASYNCHRONOUS, allOf(EventType.class));


        driverCache.put(1L, "foo");
        driverCache.put(1L, "bar");
        driverCache.remove(1L);
        driverCache.putIfAbsent(1L, "baz");
        driverCache.replace(1L, "bat");
        driverCache.replace(1L, "bat", "bag");
        driverCache.remove(1L, "bag");

        @SuppressWarnings("unchecked")
        Matcher<Iterable<? extends CacheEvent<? extends Long, ? extends String>>> expectedSequence = contains(
          created(1L, "foo"),
          updated(1L, "foo", "bar"),
          removed(1L, "bar"),
          created(1L, "baz"),
          updated(1L, "baz", "bat"),
          updated(1L, "bat", "bag"),
          removed(1L, "bag"));

        within(Duration.ofSeconds(10)).runsCleanly(() -> {
          assertThat(driverEvents, expectedSequence);
          assertThat(observerEvents, expectedSequence);
        });
      }
    }
  }

  @Test
  public void testExpiringEventSequence() throws TimeoutException {
    TestTimeSource timeSource = new TestTimeSource();

    CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder =
      newCacheManagerBuilder()
        .using(new TimeSourceConfiguration(timeSource))
        .with(cluster(CLUSTER_URI).autoCreate(s -> s.defaultServerResource("primary-server-resource")))
        .withCache(runningTest.getMethodName(), newCacheConfigurationBuilder(Long.class, String.class,
          newResourcePoolsBuilder().with(clusteredDedicated(16, MemoryUnit.MB)))
          .withExpiry(timeToLiveExpiration(Duration.ofMillis(1000))));

    try (PersistentCacheManager driver = clusteredCacheManagerBuilder.build(true)) {
      Cache<Long, String> driverCache = driver.getCache(runningTest.getMethodName(), Long.class, String.class);
      try (PersistentCacheManager observer = clusteredCacheManagerBuilder.build(true)) {
        Cache<Long, String> observerCache = observer.getCache(runningTest.getMethodName(), Long.class, String.class);

        List<CacheEvent<? extends Long, ? extends String>> driverEvents = new ArrayList<>();
        driverCache.getRuntimeConfiguration().registerCacheEventListener(driverEvents::add, EventOrdering.ORDERED, EventFiring.ASYNCHRONOUS, allOf(EventType.class));

        List<CacheEvent<? extends Long, ? extends String>> observerEvents = new ArrayList<>();
        observerCache.getRuntimeConfiguration().registerCacheEventListener(observerEvents::add, EventOrdering.ORDERED, EventFiring.ASYNCHRONOUS, allOf(EventType.class));


        driverCache.put(1L, "foo");
        timeSource.advanceTime(1100);
        driverCache.putIfAbsent(1L, "bar");
        timeSource.advanceTime(1100);
        driverCache.remove(1L);
        driverCache.put(1L, "baz");
        timeSource.advanceTime(1100);
        assertThat(driverCache.get(1L), nullValue());

        @SuppressWarnings("unchecked")
        Matcher<Iterable<? extends CacheEvent<? extends Long, ? extends String>>> expectedSequence = contains(
          created(1L, "foo"),
          expired(1L, "foo"),
          created(1L, "bar"),
          expired(1L, "bar"),
          created(1L, "baz"),
          expired(1L, "baz"));

        within(Duration.ofSeconds(10)).runsCleanly(() -> {
          assertThat(driverEvents, expectedSequence);
          assertThat(observerEvents, expectedSequence);
        });
      }
    }
  }

  private static <K, V> Matcher<CacheEvent<? extends K, ? extends V>> created(K key, V value) {
    return event(EventType.CREATED, key, null, value);
  }

  private static <K, V> Matcher<CacheEvent<? extends K, ? extends V>> updated(K key, V oldValue, V newValue) {
    return event(EventType.UPDATED, key, oldValue, newValue);
  }

  private static <K, V> Matcher<CacheEvent<? extends K, ? extends V>> removed(K key, V value) {
    return event(EventType.REMOVED, key, value, null);
  }

  private static <K, V> Matcher<CacheEvent<? extends K, ? extends V>> expired(K key, V value) {
    return event(EventType.EXPIRED, key, value, null);
  }

  private static <V, K> Matcher<CacheEvent<? extends K, ? extends V>> event(EventType type, K key, V oldValue, V newValue) {
    return new TypeSafeMatcher<CacheEvent<? extends K, ? extends V>>() {
      @Override
      protected boolean matchesSafely(CacheEvent<? extends K, ? extends V> item) {
        return type.equals(item.getType()) && key.equals(item.getKey())
          && Objects.equals(oldValue, item.getOldValue())
          && Objects.equals(newValue, item.getNewValue());
      }

      @Override
      public void describeTo(Description description) {
        description.appendText(" on '").appendValue(key).appendText("' ").appendValue(type)
          .appendText(" [").appendValue(oldValue).appendText(" => ").appendValue(newValue).appendText("]");
      }
    };
  }
}
