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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.Cache;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.AbstractValueHolder;
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
import org.ehcache.impl.config.SizedResourcePoolImpl;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.impl.internal.store.offheap.portability.AssertingOffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.impl.internal.store.shared.composites.CompositeEvictionAdvisor;
import org.ehcache.impl.internal.store.shared.composites.CompositeExpiryPolicy;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationListener;
import org.ehcache.impl.internal.store.shared.composites.CompositeInvalidationValve;
import org.ehcache.impl.internal.store.shared.composites.CompositeSerializer;
import org.ehcache.impl.internal.store.shared.composites.CompositeValue;
import org.ehcache.impl.internal.store.shared.store.StorePartition;
import org.ehcache.impl.internal.util.UnmatchedResourceType;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.UnsupportedTypeException;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.AssumptionViolatedException;
import org.junit.Test;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.QueryBuilder;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.singleton;
import static org.ehcache.config.builders.ExpiryPolicyBuilder.expiry;
import static org.ehcache.config.units.MemoryUnit.MB;
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
  private StorePartition<String, String> storePartition;
  private OffHeapStore<CompositeValue<?>, CompositeValue<?>> sharedStore;

  @After
  public void after() {
    if(storePartition != null) {
      destroyStore(sharedStore, storePartition);
    }
  }

  private StorePartition<String, String> createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super String> expiry) {
    try {
      final Map<Integer, Serializer<?>> keySerializerMap = new HashMap<>();
      final Map<Integer, Serializer<?>> valueSerializerMap = new HashMap<>();
      final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap = new HashMap<>();
      final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap = new HashMap<>();
      final Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap = new HashMap<>();
      final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap = new HashMap<>();

      ResourcePool resourcePool = new SizedResourcePoolImpl<>(ResourceType.Core.OFFHEAP, 60, MB, false);
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

      StatisticsService statisticsService = new DefaultStatisticsService();
      sharedStore = new OffHeapStore<CompositeValue<?>, CompositeValue<?>>(storeConfiguration, timeSource, new TestStoreEventDispatcher<>(), MemoryUnit.MB
        .toBytes(1), statisticsService) {
        @Override
        protected OffHeapValueHolderPortability<CompositeValue<?>> createValuePortability(Serializer<CompositeValue<?>> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      ((AuthoritativeTier) sharedStore).setInvalidationValve(new CompositeInvalidationValve(invalidationValveMap));
      ((LowerCachingTier) sharedStore).setInvalidationListener(new CompositeInvalidationListener(invalidationListenerMap));
      OffHeapStore.Provider.init(sharedStore);

      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, classLoader);
      Serializer<String> valueSerializer = serializationProvider.createValueSerializer(String.class, classLoader);

      int storeId = 1;
      keySerializerMap.put(storeId, keySerializer);
      valueSerializerMap.put(storeId, valueSerializer);
      expiryPolicyMap.put(storeId, expiry);
      evictionAdvisorMap.put(storeId, Eviction.noAdvice());

      storePartition = new StorePartition<String, String>(storeId, String.class, String.class, (Store) sharedStore);
      statisticsService.registerWithParent(sharedStore, storePartition);
      return storePartition;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  private StorePartition<String, byte[]> createAndInitStore(TimeSource timeSource, ExpiryPolicy<? super String, ? super byte[]> expiry, EvictionAdvisor<? super String, ? super byte[]> evictionAdvisor) {
    try {
      final Map<Integer, Serializer<?>> keySerializerMap = new HashMap<>();
      final Map<Integer, Serializer<?>> valueSerializerMap = new HashMap<>();
      final Map<Integer, EvictionAdvisor<?, ?>> evictionAdvisorMap = new HashMap<>();
      final Map<Integer, ExpiryPolicy<?, ?>> expiryPolicyMap = new HashMap<>();
      final Map<Integer, AuthoritativeTier.InvalidationValve> invalidationValveMap = new HashMap<>();
      final Map<Integer, CachingTier.InvalidationListener<?, ?>> invalidationListenerMap = new HashMap<>();

      ResourcePool resourcePool = new SizedResourcePoolImpl<>(ResourceType.Core.OFFHEAP, 60, MB, false);
      Store.Configuration storeConfiguration = new StoreConfigurationImpl(
        CompositeValue.class,
        CompositeValue.class,
        new CompositeEvictionAdvisor(evictionAdvisorMap),
        ClassLoading.getDefaultClassLoader(),
        new CompositeExpiryPolicy<>(expiryPolicyMap),
        new ResourcePoolsImpl(resourcePool),
        DEFAULT_DISPATCHER_CONCURRENCY,
        true,
        new CompositeSerializer(keySerializerMap),
        new CompositeSerializer(valueSerializerMap),
        null,
        false);

      StatisticsService statisticsService = new DefaultStatisticsService();
      sharedStore = new OffHeapStore<CompositeValue<?>, CompositeValue<?>>(storeConfiguration, timeSource, new TestStoreEventDispatcher<>(), MemoryUnit.MB
        .toBytes(1), statisticsService) {
        @Override
        protected OffHeapValueHolderPortability<CompositeValue<?>> createValuePortability(Serializer<CompositeValue<?>> serializer) {
          return new AssertingOffHeapValueHolderPortability<>(serializer);
        }
      };
      ((AuthoritativeTier) sharedStore).setInvalidationValve(new CompositeInvalidationValve(invalidationValveMap));
      ((LowerCachingTier) sharedStore).setInvalidationListener(new CompositeInvalidationListener(invalidationListenerMap));
      OffHeapStore.Provider.init(sharedStore);

      SerializationProvider serializationProvider = new DefaultSerializationProvider(null);
      serializationProvider.start(providerContaining());
      ClassLoader classLoader = getClass().getClassLoader();
      Serializer<String> keySerializer = serializationProvider.createKeySerializer(String.class, classLoader);
      Serializer<byte[]> valueSerializer = serializationProvider.createValueSerializer(byte[].class, classLoader);

      int storeId = 1;
      keySerializerMap.put(storeId, keySerializer);
      valueSerializerMap.put(storeId, valueSerializer);
      expiryPolicyMap.put(storeId, expiry);
      evictionAdvisorMap.put(storeId, evictionAdvisor);

      StorePartition<String, byte[]> storePartition = new StorePartition<>(storeId, String.class, byte[].class, (Store) sharedStore);
      statisticsService.registerWithParent(sharedStore, storePartition);
      return storePartition;
    } catch (UnsupportedTypeException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testRankAuthority() {
    OffHeapStore.Provider provider = new OffHeapStore.Provider();
    assertThat(provider.rankAuthority(singleton(ResourceType.Core.OFFHEAP), EMPTY_LIST), is(1));
    assertThat(provider.rankAuthority(singleton(new UnmatchedResourceType()), EMPTY_LIST), is(0));
  }

  @Test
  public void testRank() {
    OffHeapStore.Provider provider = new OffHeapStore.Provider();
    assertRank(provider, 1, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 0, new UnmatchedResourceType());
    assertRank(provider, 0, ResourceType.Core.OFFHEAP, new UnmatchedResourceType());
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {
    assertThat(provider.rank(
        new HashSet<>(Arrays.asList(resources)),
        Collections.<ServiceConfiguration<?, ?>>emptyList()),
      is(expectedRank));
  }

  protected void destroyStore(OffHeapStore<CompositeValue<?>, CompositeValue<?>> sharedStore, StorePartition<?, ?> store) {
    if (sharedStore != null) {
      OffHeapStore.Provider.close(sharedStore);
    }
    StatisticsManager.nodeFor(store).clean();
  }

  @Test
  public void testGetAndRemoveNoValue() throws Exception {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.noExpiration());
    assertThat(storePartition.getAndRemove("1"), is(nullValue()));
    validateStats(sharedStore, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.MISS));
    //validateStats(storePartition, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.MISS));
  }

  @Test
  public void testGetAndRemoveValue() throws Exception {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.noExpiration());
    storePartition.put("1", "one");
    assertThat(storePartition.getAndRemove("1").get(), equalTo("one"));
    validateStats(sharedStore, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.HIT_REMOVED));
    //validateStats(storePartition, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.HIT_REMOVED));
    assertThat(storePartition.get("1"), is(nullValue()));
  }

  @Test
  public void testGetAndRemoveExpiredElementReturnsNull() throws Exception {
    throw new AssumptionViolatedException("Fix this test");
//    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
//    assertThat(storePartition.getAndRemove("1"), is(nullValue()));
//    storePartition.put("1", "one");
//    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
//    storePartition.setInvalidationListener((key, valueHolder) -> {
//      valueHolder.get();
//      invalidated.set(valueHolder);
//    });
//    timeSource.advanceTime(20);
//    assertThat(storePartition.getAndRemove("1"), is(nullValue()));
//    assertThat(invalidated.get().get(), equalTo("one"));
//    assertThat(invalidated.get().isExpired(timeSource.getTimeMillis()), is(true));
//    assertThat(getExpirationStatistic(sharedStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
//    assumeThat(getExpirationStatistic(storePartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testInstallMapping() throws Exception {
    throw new AssumptionViolatedException("Fix this test");
//    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
//    assertThat(storePartition.installMapping("1", key -> new SimpleValueHolder<>("one", timeSource.getTimeMillis(), 15)).get(), equalTo("one"));
//    validateStats(sharedStore, EnumSet.of(LowerCachingTierOperationsOutcome.InstallMappingOutcome.PUT));
//    //validateStats(storePartition, EnumSet.of(LowerCachingTierOperationsOutcome.InstallMappingOutcome.PUT));
//    timeSource.advanceTime(20);
//    try {
//      storePartition.installMapping("1", key -> new SimpleValueHolder<>("un", timeSource.getTimeMillis(), 15));
//      fail("expected AssertionError");
//    } catch (AssertionError ae) {
//      // expected
//    }
  }

  @Test
  public void testInvalidateKeyAbsent() throws Exception {
    throw new AssumptionViolatedException("Fix this test");
//    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
//    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
//    storePartition.setInvalidationListener((key, valueHolder) -> invalidated.set(valueHolder));
//    storePartition.invalidate("1");
//    assertThat(invalidated.get(), is(nullValue()));
//    validateStats(sharedStore, EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.MISS));
//    //validateStats(storePartition, EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.MISS));
  }

  @Test
  public void testInvalidateKeyPresent() throws Exception {
    throw new AssumptionViolatedException("Fix this test");
//    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
//    storePartition.put("1", "one");
//    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
//    storePartition.setInvalidationListener((key, valueHolder) -> {
//      valueHolder.get();
//      invalidated.set(valueHolder);
//    });
//    storePartition.invalidate("1");
//    assertThat(invalidated.get().get(), equalTo("one"));
//    validateStats(sharedStore, EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.REMOVED));
//    //validateStats(storePartition, EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.REMOVED));
//    assertThat(storePartition.get("1"), is(nullValue()));
  }

  @Test
  public void testClear() throws Exception {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
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
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));
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
    StorePartition<String, byte[]> storePartition = createAndInitStore(timeSource, expiry, evictionAdvisor);
    try {
      StoreEventListener<String, byte[]> listener = uncheckedGenericMock(StoreEventListener.class);
      storePartition.getStoreEventSource().addEventListener(listener);
      byte[] value = getBytes(MemoryUnit.KB.toBytes(200));
      storePartition.put("key1", value);
      storePartition.put("key2", value);
      storePartition.put("key3", value);
      storePartition.put("key4", value);
      storePartition.put("key5", value);
      storePartition.put("key6", value);
      Matcher<StoreEvent<String, byte[]>> matcher = eventType(EventType.EVICTED);
      verify(listener, atLeast(1)).onEvent(argThat(matcher));
    } finally {
      destroyStore(sharedStore, storePartition);
    }
  }

  @Test
  public void testFlushUpdatesAccessStats() throws StoreAccessException {
    throw new AssumptionViolatedException("Fix this test");
//    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L));
//    storePartition = createAndInitStore(timeSource, expiry);
//    try {
//      final String key = "foo";
//      final String value = "bar";
//      storePartition.put(key, value);
//      final Store.ValueHolder<String> firstValueHolder = storePartition.getAndFault(key);
//      storePartition.put(key, value);
//      final Store.ValueHolder<String> secondValueHolder = storePartition.getAndFault(key);
//      timeSource.advanceTime(10);
//      ((AbstractValueHolder) firstValueHolder).accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, () -> value));
//      timeSource.advanceTime(10);
//      ((AbstractValueHolder) secondValueHolder).accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, () -> value));
//      assertThat(storePartition.flush(key, new DelegatingValueHolder<>(firstValueHolder)), is(false));
//      assertThat(storePartition.flush(key, new DelegatingValueHolder<>(secondValueHolder)), is(true));
//      timeSource.advanceTime(10); // this should NOT affect
//      assertThat(storePartition.getAndFault(key).lastAccessTime(), is(secondValueHolder.creationTime() + 20));
//    } finally {
//      destroyStore(sharedStore, storePartition);
//    }
  }

  @Test
  public void testExpiryEventFiredOnExpiredCachedEntry() throws StoreAccessException {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
    final List<String> expiredKeys = new ArrayList<>();
    storePartition.getStoreEventSource().addEventListener(event -> {
      if (event.getType() == EventType.EXPIRED) {
        //expiredKeys.add(event.getKey());
        CompositeValue<String> compositeKey = (CompositeValue<String>)(Object)event.getKey();
        expiredKeys.add(compositeKey.getValue());
      }
    });
    storePartition.put("key1", "value1");
    storePartition.put("key2", "value2");
    storePartition.get("key1");   // Bring the entry to the caching tier
    timeSource.advanceTime(11);   // Expire the elements
    storePartition.get("key1");
    storePartition.get("key2");
    assertThat(expiredKeys, containsInAnyOrder("key1", "key2"));
    assertThat(getExpirationStatistic(sharedStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(2L));
    assumeThat(getExpirationStatistic(storePartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(2L));
  }

  @Test
  public void testGetWithExpiryOnAccess() throws Exception {
    storePartition = createAndInitStore(timeSource, expiry().access(Duration.ZERO).build());
    storePartition.put("key", "value");
    final AtomicReference<CompositeValue<String>> expired = new AtomicReference<>();
    storePartition.getStoreEventSource().addEventListener(event -> {
      if (event.getType() == EventType.EXPIRED) {
        expired.set((CompositeValue<String>)(Object)event.getKey());  //xxx
      }
    });
    assertThat(storePartition.get("key"), valueHeld("value"));
    assertThat(((CompositeValue<?>)expired.get()).getValue(), is("key"));
  }

  @Test
  public void testExpiryCreateException() throws Exception {
    storePartition = createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
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
    storePartition = createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
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
    storePartition = createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
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
  public void testGetAndFaultOnExpiredEntry() throws StoreAccessException {
    throw new AssumptionViolatedException("Fix this test");
//    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
//    try {
//      storePartition.put("key", "value");
//      timeSource.advanceTime(20L);
//      Store.ValueHolder<String> valueHolder = storePartition.getAndFault("key");
//      assertThat(valueHolder, nullValue());
//      assertThat(getExpirationStatistic(sharedStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
//      assumeThat(getExpirationStatistic(storePartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
//    } finally {
//      destroyStore(sharedStore, storePartition);
//    }
  }

  @Test
  public void testComputeExpiresOnAccess() throws StoreAccessException {
    timeSource.advanceTime(1000L);
    storePartition = createAndInitStore(timeSource, expiry().access(Duration.ZERO).update(Duration.ZERO).build());
    storePartition.put("key", "value");
    Store.ValueHolder<String> result = storePartition.computeAndGet("key", (s, s2) -> s2, () -> false, () -> false);
    assertThat(result, valueHeld("value"));
  }

  @Test
  public void testComputeExpiresOnUpdate() throws StoreAccessException {
    timeSource.advanceTime(1000L);
    storePartition = createAndInitStore(timeSource, expiry().access(Duration.ZERO).update(Duration.ZERO).build());
    storePartition.put("key", "value");
    Store.ValueHolder<String> result = storePartition.computeAndGet("key", (s, s2) -> "newValue", () -> false, () -> false);
    assertThat(result, valueHeld("newValue"));
  }

  @Test
  public void testComputeOnExpiredEntry() throws StoreAccessException {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
    storePartition.put("key", "value");
    timeSource.advanceTime(20L);
    storePartition.getAndCompute("key", (mappedKey, mappedValue) -> {
      assertThat(mappedKey, is("key"));
      assertThat(mappedValue, nullValue());
      return "value2";
    });
    assertThat(getExpirationStatistic(sharedStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
    assumeThat(getExpirationStatistic(storePartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testComputeIfAbsentOnExpiredEntry() throws StoreAccessException {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
    storePartition.put("key", "value");
    timeSource.advanceTime(20L);
    storePartition.computeIfAbsent("key", mappedKey -> {
      assertThat(mappedKey, is("key"));
      return "value2";
    });
    assertThat(getExpirationStatistic(sharedStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
    assumeThat(getExpirationStatistic(storePartition).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testIteratorDoesNotSkipOrExpiresEntries() throws Exception {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));
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
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));
    storePartition.put("key1", "value1");
    timeSource.advanceTime(11L);
    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = storePartition.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next().getKey(), equalTo("key1"));
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorWithSingleNonExpiredEntry() throws Exception {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));
    storePartition.put("key1", "value1");
    timeSource.advanceTime(5L);
    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = storePartition.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next().getKey(), is("key1"));
  }

  @Test
  public void testIteratorOnEmptyStore() throws Exception {
    storePartition = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));
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
  private OperationStatistic<StoreOperationOutcomes.ExpirationOutcome> getExpirationStatistic(Store<?, ?> store) {
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

  private static class TestTimeSource implements TimeSource {

    private long time = 0;

    @Override
    public long getTimeMillis() {
      return time;
    }

    public void advanceTime(long step) {
      time += step;
    }
  }

  public static class DelegatingValueHolder<T> implements Store.ValueHolder<T> {

    private final Store.ValueHolder<T> valueHolder;

    public DelegatingValueHolder(final Store.ValueHolder<T> valueHolder) {
      this.valueHolder = valueHolder;
    }

    @Override
    public T get() {
      return valueHolder.get();
    }

    @Override
    public long creationTime() {
      return valueHolder.creationTime();
    }

    @Override
    public long expirationTime() {
      return valueHolder.expirationTime();
    }

    @Override
    public boolean isExpired(long expirationTime) {
      return valueHolder.isExpired(expirationTime);
    }

    @Override
    public long lastAccessTime() {
      return valueHolder.lastAccessTime();
    }

    @Override
    public long getId() {
      return valueHolder.getId();
    }
  }

  static class SimpleValueHolder<T> extends AbstractValueHolder<T> {

    private final T value;

    public SimpleValueHolder(T v, long creationTime, long expirationTime) {
      super(-1, creationTime, expirationTime);
      this.value = v;
    }

    @Override
    public T get() {
      return value;
    }

    @Override
    public long creationTime() {
      return 0;
    }

    @Override
    public long expirationTime() {
      return 0;
    }

    @Override
    public boolean isExpired(long expirationTime) {
      return false;
    }

    @Override
    public long lastAccessTime() {
      return 0;
    }

    @Override
    public long getId() {
      return 0;
    }
  }
}
