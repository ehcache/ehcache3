/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.Cache;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourceType;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.spi.store.tiering.LowerCachingTier;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.statistics.LowerCachingTierOperationsOutcome;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.store.StoreConfigurationImpl;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.event.EventType;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.ResourcePoolsImpl;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.impl.internal.store.offheap.portability.AssertingOffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.shared.authoritative.AuthoritativeTierPartition;
import org.ehcache.impl.internal.store.shared.caching.lower.LowerCachingTierPartition;
import org.ehcache.impl.internal.store.shared.composites.CompositeEvictionAdvisor;
import org.ehcache.impl.internal.store.shared.composites.CompositeExpiryPolicy;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationListener;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationValve;
import org.ehcache.impl.internal.store.shared.composites.CompositeSerializer;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.internal.store.shared.store.StorePartition;
import org.ehcache.impl.internal.util.UnmatchedResourceType;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Test;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.QueryBuilder;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singleton;
import static org.ehcache.config.ResourceType.Core.OFFHEAP;
import static org.ehcache.config.builders.ExpiryPolicyBuilder.expiry;
import static org.ehcache.core.config.store.StoreEventSourceConfiguration.DEFAULT_DISPATCHER_CONCURRENCY;
import static org.ehcache.impl.internal.spi.TestServiceProvider.providerContaining;
import static org.ehcache.impl.internal.util.Matchers.valueHeld;
import static org.ehcache.impl.internal.util.StatisticsTestUtils.validateStats;
import static org.ehcache.test.MockitoUtil.uncheckedGenericMock;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.assumeThat;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

@SuppressWarnings({"unchecked", "rawtypes"})
public class SharedOffHeapStoreTest {
  private final TestTimeSource timeSource = new TestTimeSource();
  private SharedOffHeapStoreTest.SharedOffHeapStore sharedStore;
  private StorePartition<String, String> storePartition;
  private StorePartition<String, byte[]> storePartitionWithBytes;

  @After
  public void after() {
    if (sharedStore != null) {
      sharedStore.destroy(storePartition);
      sharedStore.destroy(storePartitionWithBytes);
    }
  }

  private void createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super String> expiry) {
    createAndInitStore(timeSource, expiry, Eviction.noAdvice());
  }

  private void createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super String> expiry, EvictionAdvisor evictionAdvisor) {
    try {
      sharedStore = new SharedOffHeapStoreTest.SharedOffHeapStore(timeSource);
      storePartition = sharedStore.createStorePartition(1, String.class, String.class, evictionAdvisor, expiry);
      storePartitionWithBytes = sharedStore.createStorePartition(2, String.class, byte[].class, evictionAdvisor, expiry);
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRankAuthority() {
    OffHeapStore.Provider provider = new OffHeapStore.Provider();
    assertThat(provider.rankAuthority(singleton(OFFHEAP), EMPTY_LIST), is(1));
    assertThat(provider.rankAuthority(singleton(new UnmatchedResourceType()), EMPTY_LIST), is(0));
  }

  @Test
  public void testRank() {
    OffHeapStore.Provider provider = new OffHeapStore.Provider();
    assertRank(provider, 1, OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, new UnmatchedResourceType());
    assertRank(provider, 0, OFFHEAP, new UnmatchedResourceType());
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {
    assertThat(provider.rank(
        new HashSet<>(Arrays.asList(resources)),
        Collections.emptyList()),
      is(expectedRank));
  }


  @Test
  public void testGetAndRemoveNoValue() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.noExpiration());
    assertThat(storePartition.getAndRemove("1"), is(nullValue()));
    validateStats(sharedStore.sharedStore, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.MISS));
    //validateStats(storePartition, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.MISS));
  }

  @Test
  public void testGetAndRemoveValue() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.noExpiration());
    storePartition.put("1", "one");
    assertThat(storePartition.getAndRemove("1").get(), equalTo("one"));
    validateStats(sharedStore.sharedStore, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.HIT_REMOVED));
    //validateStats(storePartition, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.HIT_REMOVED));
    assertThat(storePartition.get("1"), is(nullValue()));
  }

  @Test
  public void testClear() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
    storePartition.put("1", "one");
    storePartition.put("2", "two");
    storePartition.put("3", "three");
    storePartition.clear();
    assertThat(storePartition.get("1"), is(nullValue()));
    assertThat(storePartition.get("2"), is(nullValue()));
    assertThat(storePartition.get("3"), is(nullValue()));
  }

  @Test
  public void testWriteBackOfValueHolder() throws StoreAccessException {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
    storePartition.put("key1", "value1");
    timeSource.advanceTime(10);
    Store.ValueHolder<String> valueHolder = storePartition.get("key1");
    assertThat(valueHolder.lastAccessTime(), is(10L));
    timeSource.advanceTime(10);
    assertThat(storePartition.get("key1"), notNullValue());
    timeSource.advanceTime(16);
    assertThat(storePartition.get("key1"), nullValue());
  }

  @Test
  public void testEvictionAdvisor() throws StoreAccessException {
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L));
    EvictionAdvisor<String, byte[]> evictionAdvisor = (key, value) -> true;
    performEvictionTest(timeSource, expiry, evictionAdvisor);
  }

  @Test
  public void testBrokenEvictionAdvisor() throws StoreAccessException {
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L));
    EvictionAdvisor<String, byte[]> evictionAdvisor = (key, value) -> {
      throw new UnsupportedOperationException("Broken advisor!");
    };
    performEvictionTest(timeSource, expiry, evictionAdvisor);
  }

  private void performEvictionTest(TestTimeSource timeSource, ExpiryPolicy<Object, Object> expiry, EvictionAdvisor<String, byte[]> evictionAdvisor) throws StoreAccessException {
    createAndInitStore(timeSource, expiry, evictionAdvisor);
    StoreEventListener<String, byte[]> listener = uncheckedGenericMock(StoreEventListener.class);
    storePartitionWithBytes.getStoreEventSource().addEventListener(listener);
    byte[] value = getBytes(MemoryUnit.KB.toBytes(200));
    storePartitionWithBytes.put("key1", value);
    storePartitionWithBytes.put("key2", value);
    storePartitionWithBytes.put("key3", value);
    storePartitionWithBytes.put("key4", value);
    storePartitionWithBytes.put("key5", value);
    storePartitionWithBytes.put("key6", value);
    Matcher<StoreEvent<String, byte[]>> matcher = eventType(EventType.EVICTED);
    verify(listener, atLeast(1)).onEvent(argThat(matcher));
  }

  @Test
  public void testExpiryEventFiredOnExpiredCachedEntry() throws StoreAccessException {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
    final List<String> expiredKeys = new ArrayList<>();
    storePartition.getStoreEventSource().addEventListener(event -> {
      if (event.getType() == EventType.EXPIRED) {
        expiredKeys.add(event.getKey());
      }
    });
    storePartition.put("key1", "value1");
    storePartition.put("key2", "value2");
    storePartition.get("key1");   // Bring the entry to the caching tier
    timeSource.advanceTime(11);   // Expire the elements
    storePartition.get("key1");
    storePartition.get("key2");
    assertThat(expiredKeys, containsInAnyOrder("key1", "key2"));
    assertThat(getExpirationStatistic(sharedStore.sharedStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(2L));
    assumeThat(getExpirationStatistic(storePartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(2L));
  }

  @Test
  public void testGetWithExpiryOnAccess() throws Exception {
    createAndInitStore(timeSource, expiry().access(Duration.ZERO).build());
    storePartition.put("key", "value");
    final AtomicReference<String> expired = new AtomicReference<>();
    storePartition.getStoreEventSource().addEventListener(event -> {
      if (event.getType() == EventType.EXPIRED) {
        expired.set(event.getKey());
      }
    });
    assertThat(storePartition.get("key"), valueHeld("value"));
    assertThat(expired.get(), is("key"));
  }

  @Test
  public void testExpiryCreateException() throws Exception {
    createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        throw new RuntimeException();
      }

      @Override
      public Duration getExpiryForAccess(String key, Supplier<? extends String> value) {
        throw new AssertionError();
      }

      @Override
      public Duration getExpiryForUpdate(String key, Supplier<? extends String> oldValue, String newValue) {
        throw new AssertionError();
      }
    });
    storePartition.put("key", "value");
    assertNull(storePartition.get("key"));
  }

  @Test
  public void testExpiryAccessException() throws Exception {
    createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return ExpiryPolicy.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(String key, Supplier<? extends String> value) {
        throw new RuntimeException();
      }

      @Override
      public Duration getExpiryForUpdate(String key, Supplier<? extends String> oldValue, String newValue) {
        return null;
      }
    });
    storePartition.put("key", "value");
    assertThat(storePartition.get("key"), valueHeld("value"));
    assertNull(storePartition.get("key"));
  }

  @Test
  public void testExpiryUpdateException() throws Exception {
    createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return ExpiryPolicy.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(String key, Supplier<? extends String> value) {
        return ExpiryPolicy.INFINITE;
      }

      @Override
      public Duration getExpiryForUpdate(String key, Supplier<? extends String> oldValue, String newValue) {
        if (timeSource.getTimeMillis() > 0) {
          throw new RuntimeException();
        }
        return ExpiryPolicy.INFINITE;
      }
    });
    storePartition.put("key", "value");
    assertThat(storePartition.get("key").get(), is("value"));
    timeSource.advanceTime(1000);
    storePartition.put("key", "newValue");
    assertNull(storePartition.get("key"));
  }

  @Test
  public void testComputeExpiresOnAccess() throws StoreAccessException {
    timeSource.advanceTime(1000L);
    createAndInitStore(timeSource, expiry().access(Duration.ZERO).update(Duration.ZERO).build());
    storePartition.put("key", "value");
    Store.ValueHolder<String> result = storePartition.computeAndGet("key", (s, s2) -> s2, () -> false, () -> false);
    assertThat(result, valueHeld("value"));
  }

  @Test
  public void testComputeExpiresOnUpdate() throws StoreAccessException {
    timeSource.advanceTime(1000L);
    createAndInitStore(timeSource, expiry().access(Duration.ZERO).update(Duration.ZERO).build());
    storePartition.put("key", "value");
    Store.ValueHolder<String> result = storePartition.computeAndGet("key", (s, s2) -> "newValue", () -> false, () -> false);
    assertThat(result, valueHeld("newValue"));
  }

  @Test
  public void testComputeOnExpiredEntry() throws StoreAccessException {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
    storePartition.put("key", "value");
    timeSource.advanceTime(20L);
    storePartition.getAndCompute("key", (mappedKey, mappedValue) -> {
      assertThat(mappedKey, is("key"));
      assertThat(mappedValue, nullValue());
      return "value2";
    });
    assertThat(getExpirationStatistic(sharedStore.sharedStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
    assumeThat(getExpirationStatistic(storePartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testComputeIfAbsentOnExpiredEntry() throws StoreAccessException {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
    storePartition.put("key", "value");
    timeSource.advanceTime(20L);
    storePartition.computeIfAbsent("key", mappedKey -> {
      assertThat(mappedKey, is("key"));
      return "value2";
    });
    assertThat(getExpirationStatistic(sharedStore.sharedStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
    assumeThat(getExpirationStatistic(storePartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testIteratorDoesNotSkipOrExpiresEntries() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));
    storePartition.put("key1", "value1");
    storePartition.put("key2", "value2");
    timeSource.advanceTime(11L);
    storePartition.put("key3", "value3");
    storePartition.put("key4", "value4");
    final List<String> expiredKeys = new ArrayList<>();
    storePartition.getStoreEventSource().addEventListener(event -> {
      if (event.getType() == EventType.EXPIRED) {
        expiredKeys.add(event.getKey());
      }
    });
    List<String> iteratedKeys = new ArrayList<>();
    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = storePartition.iterator();
    while(iterator.hasNext()) {
      iteratedKeys.add(iterator.next().getKey());
    }
    assertThat(iteratedKeys, containsInAnyOrder("key1", "key2", "key3", "key4"));
    assertThat(expiredKeys.isEmpty(), is(true));
  }

  @Test
  public void testIteratorWithSingleExpiredEntry() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));
    storePartition.put("key1", "value1");
    timeSource.advanceTime(11L);
    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = storePartition.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next().getKey(), equalTo("key1"));
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorWithSingleNonExpiredEntry() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));
    storePartition.put("key1", "value1");
    timeSource.advanceTime(5L);
    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = storePartition.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next().getKey(), is("key1"));
  }

  @Test
  public void testIteratorOnEmptyStore() throws Exception {
    createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));
    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = storePartition.iterator();
    assertFalse(iterator.hasNext());
  }

  public static <K, V> Matcher<StoreEvent<K, V>> eventType(final EventType type) {
    return new TypeSafeMatcher<StoreEvent<K, V>>() {
      @Override
      protected boolean matchesSafely(StoreEvent<K, V> item) {
        return item.getType().equals(type);
      }

      @Override
      public void describeTo(Description description) {
        description.appendText("store event of type '").appendValue(type).appendText("'");
      }
    };
  }

  @SuppressWarnings("unchecked")
  public static OperationStatistic<StoreOperationOutcomes.ExpirationOutcome> getExpirationStatistic(Store<?, ?> store) {
    StatisticsManager statisticsManager = new StatisticsManager();
    statisticsManager.root(store);
    TreeNode treeNode = statisticsManager.queryForSingleton(QueryBuilder.queryBuilder()
        .descendants()
        .filter(org.terracotta.context.query.Matchers.context(
          org.terracotta.context.query.Matchers.allOf(
            org.terracotta.context.query.Matchers.identifier(org.terracotta.context.query.Matchers.subclassOf(OperationStatistic.class)),
            org.terracotta.context.query.Matchers.attributes(org.terracotta.context.query.Matchers.hasAttribute("name", "expiration")))))
            //org.terracotta.context.query.Matchers.attributes(hasTag(((BaseStore)store).getStatisticsTag())))))
        .build());
      return (OperationStatistic<StoreOperationOutcomes.ExpirationOutcome>) treeNode.getContext().attributes().get("this");
  }

  private byte[] getBytes(long valueLength) {
    assertThat(valueLength, lessThan((long) Integer.MAX_VALUE));
    int valueLengthInt = (int) valueLength;
    byte[] value = new byte[valueLengthInt];
    new Random().nextBytes(value);
    return value;
  }

  public static class SharedOffHeapStore<K, V> {
    private final OffHeapStore<CompositeValue<K>, CompositeValue<V>> sharedStore;
    private final Map<Integer, Serializer<?>> keySerializerMap = new HashMap<>();
    private final Map<Integer, Serializer<?>> valueSerializerMap = new HashMap<>();
    private final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap = new HashMap<>();
    private final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap = new HashMap<>();
    private final Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap = new HashMap<>();
    private final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap = new HashMap<>();
    private final StatisticsService statisticsService = new DefaultStatisticsService();

    SharedOffHeapStore(TimeSource timeSource) {
      this(timeSource, 60L, 1L);
    }

    SharedOffHeapStore(TimeSource timeSource, Comparable<Long> capacity, Long sizeInBytes) {
      SizedResourcePool resourcePool = ResourcePoolsBuilder.newResourcePoolsBuilder()
        .offheap(capacity == null ? 10L : (long)capacity, MemoryUnit.MB).build().getPoolForResource(OFFHEAP);
      Store.Configuration storeConfiguration = new StoreConfigurationImpl(
        CompositeValue.class,
        CompositeValue.class,
        new CompositeEvictionAdvisor(evictionAdvisorMap),
        ClassLoading.getDefaultClassLoader(),
        new CompositeExpiryPolicy(expiryPolicyMap),
        new ResourcePoolsImpl(resourcePool),
        DEFAULT_DISPATCHER_CONCURRENCY,
        true,
        new CompositeSerializer(keySerializerMap),
        new CompositeSerializer(valueSerializerMap),
        null,
        false);
      sharedStore = new OffHeapStore<CompositeValue<K>, CompositeValue<V>>(storeConfiguration, timeSource, new TestStoreEventDispatcher<>(), MemoryUnit.MB
        .toBytes(sizeInBytes == null ? resourcePool.getSize() : sizeInBytes), statisticsService) {
        @Override
        protected OffHeapValueHolderPortability<CompositeValue<V>> createValuePortability(Serializer<CompositeValue<V>> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      ((AuthoritativeTier) sharedStore).setInvalidationValve(new CompositeInvalidationValve(invalidationValveMap));
      ((LowerCachingTier) sharedStore).setInvalidationListener(new CompositeInvalidationListener(invalidationListenerMap));
      OffHeapStore.Provider.init(sharedStore);
    }

    public OffHeapStore<CompositeValue<K>, CompositeValue<V>> getSharedStore() {
      return sharedStore;
    }

    public StorePartition<K, V> createStorePartition(int partitionId, Class<K> keyType, Class<V> valueType,
                                                     EvictionAdvisor evictionAdvisor,
                                                     ExpiryPolicy<? super String, ? super String> expiry) throws UnsupportedTypeException {
      setupMaps(partitionId, keyType, valueType, evictionAdvisor, expiry);
      StorePartition<K, V> partition = new StorePartition<>(ResourceType.Core.OFFHEAP, partitionId, keyType, valueType, sharedStore);
      statisticsService.registerWithParent(sharedStore, partition);
      return partition;
    }

    public AuthoritativeTierPartition<K, V> createAuthoritativeTierPartition(int partitionId, Class<K> keyType, Class<V> valueType,
                                                                             EvictionAdvisor evictionAdvisor,
                                                                             ExpiryPolicy<? super String, ? super String> expiry) throws UnsupportedTypeException {
      setupMaps(partitionId, keyType, valueType, evictionAdvisor, expiry);
      AuthoritativeTierPartition<K, V> partition = new AuthoritativeTierPartition(ResourceType.Core.OFFHEAP, partitionId, keyType, valueType, sharedStore);
      statisticsService.registerWithParent(sharedStore, partition);
      return partition;
    }

    public LowerCachingTierPartition<K, V> createLowerCachingTierPartition(int partitionId, Class<K> keyType, Class<V> valueType,
                                                                           EvictionAdvisor evictionAdvisor,
                                                                           ExpiryPolicy<? super String, ? super String> expiry) throws UnsupportedTypeException {
      setupMaps(partitionId, keyType, valueType, evictionAdvisor, expiry);
      LowerCachingTierPartition<K, V> partition = new LowerCachingTierPartition(ResourceType.Core.OFFHEAP, partitionId, sharedStore, invalidationListenerMap);
      statisticsService.registerWithParent(sharedStore, partition);
      return partition;
    }

    private void setupMaps(int partitionId, Class<K> keyType, Class<V> valueType,
                           EvictionAdvisor evictionAdvisor,
                           ExpiryPolicy<? super String, ? super String> expiry) throws UnsupportedTypeException {
      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      ClassLoader classLoader = getClass().getClassLoader();
      keySerializerMap.put(partitionId, serializationProvider.createValueSerializer(keyType, classLoader));
      valueSerializerMap.put(partitionId, serializationProvider.createValueSerializer(valueType, classLoader));
      expiryPolicyMap.put(partitionId, expiry);
      evictionAdvisorMap.put(partitionId, evictionAdvisor);
    }

    public void destroy(Object partition) {
      if (partition != null) {
        StatisticsManager.nodeFor(partition).clean();
      }
        OffHeapStore.Provider.close(sharedStore);
    }
  }
}
