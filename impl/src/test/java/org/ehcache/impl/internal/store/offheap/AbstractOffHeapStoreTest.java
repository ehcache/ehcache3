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
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.EventType;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.store.AbstractValueHolder;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.core.statistics.LowerCachingTierOperationsOutcome;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.expiry.ExpiryPolicy;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Test;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.QueryBuilder;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;

import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.ehcache.config.builders.ExpiryPolicyBuilder.expiry;
import static org.ehcache.impl.internal.util.Matchers.valueHeld;
import static org.ehcache.impl.internal.util.StatisticsTestUtils.validateStats;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 *
 * @author cdennis
 */
public abstract class AbstractOffHeapStoreTest {

  private TestTimeSource timeSource = new TestTimeSource();
  private AbstractOffHeapStore<String, String> offHeapStore;

  @After
  public void after() {
    if(offHeapStore != null) {
      destroyStore(offHeapStore);
    }
  }

  @Test
  public void testGetAndRemoveNoValue() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.noExpiration());

    assertThat(offHeapStore.getAndRemove("1"), is(nullValue()));
    validateStats(offHeapStore, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.MISS));
  }

  @Test
  public void testGetAndRemoveValue() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.noExpiration());

    offHeapStore.put("1", "one");
    assertThat(offHeapStore.getAndRemove("1").get(), equalTo("one"));
    validateStats(offHeapStore, EnumSet.of(LowerCachingTierOperationsOutcome.GetAndRemoveOutcome.HIT_REMOVED));
    assertThat(offHeapStore.get("1"), is(nullValue()));
  }

  @Test
  public void testGetAndRemoveExpiredElementReturnsNull() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));

    assertThat(offHeapStore.getAndRemove("1"), is(nullValue()));

    offHeapStore.put("1", "one");

    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
    offHeapStore.setInvalidationListener((key, valueHolder) -> {
      valueHolder.get();
      invalidated.set(valueHolder);
    });

    timeSource.advanceTime(20);
    assertThat(offHeapStore.getAndRemove("1"), is(nullValue()));
    assertThat(invalidated.get().get(), equalTo("one"));
    assertThat(invalidated.get().isExpired(timeSource.getTimeMillis()), is(true));
    assertThat(getExpirationStatistic(offHeapStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testInstallMapping() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));

    assertThat(offHeapStore.installMapping("1", key -> new SimpleValueHolder<>("one", timeSource.getTimeMillis(), 15)).get(), equalTo("one"));

    validateStats(offHeapStore, EnumSet.of(LowerCachingTierOperationsOutcome.InstallMappingOutcome.PUT));

    timeSource.advanceTime(20);

    try {
      offHeapStore.installMapping("1", key -> new SimpleValueHolder<>("un", timeSource.getTimeMillis(), 15));
      fail("expected AssertionError");
    } catch (AssertionError ae) {
      // expected
    }
  }

  @Test
  public void testInvalidateKeyAbsent() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));

    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
    offHeapStore.setInvalidationListener((key, valueHolder) -> invalidated.set(valueHolder));

    offHeapStore.invalidate("1");
    assertThat(invalidated.get(), is(nullValue()));
    validateStats(offHeapStore, EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.MISS));
  }

  @Test
  public void testInvalidateKeyPresent() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));

    offHeapStore.put("1", "one");

    final AtomicReference<Store.ValueHolder<String>> invalidated = new AtomicReference<>();
    offHeapStore.setInvalidationListener((key, valueHolder) -> {
      valueHolder.get();
      invalidated.set(valueHolder);
    });

    offHeapStore.invalidate("1");
    assertThat(invalidated.get().get(), equalTo("one"));
    validateStats(offHeapStore, EnumSet.of(LowerCachingTierOperationsOutcome.InvalidateOutcome.REMOVED));

    assertThat(offHeapStore.get("1"), is(nullValue()));
  }

  @Test
  public void testClear() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));

    offHeapStore.put("1", "one");
    offHeapStore.put("2", "two");
    offHeapStore.put("3", "three");
    offHeapStore.clear();

    assertThat(offHeapStore.get("1"), is(nullValue()));
    assertThat(offHeapStore.get("2"), is(nullValue()));
    assertThat(offHeapStore.get("3"), is(nullValue()));
  }

  @Test
  public void testWriteBackOfValueHolder() throws StoreAccessException {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L)));

    offHeapStore.put("key1", "value1");
    timeSource.advanceTime(10);
    OffHeapValueHolder<String> valueHolder = (OffHeapValueHolder<String>)offHeapStore.get("key1");
    assertThat(valueHolder.lastAccessTime(), is(10L));
    timeSource.advanceTime(10);
    assertThat(offHeapStore.get("key1"), notNullValue());
    timeSource.advanceTime(16);
    assertThat(offHeapStore.get("key1"), nullValue());
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

  @Test
  public void testFlushUpdatesAccessStats() throws StoreAccessException {
    ExpiryPolicy<Object, Object> expiry = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(15L));
    offHeapStore = createAndInitStore(timeSource, expiry);
    try {
      final String key = "foo";
      final String value = "bar";
      offHeapStore.put(key, value);
      final Store.ValueHolder<String> firstValueHolder = offHeapStore.getAndFault(key);
      offHeapStore.put(key, value);
      final Store.ValueHolder<String> secondValueHolder = offHeapStore.getAndFault(key);
      timeSource.advanceTime(10);
      ((AbstractValueHolder) firstValueHolder).accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, () -> value));
      timeSource.advanceTime(10);
      ((AbstractValueHolder) secondValueHolder).accessed(timeSource.getTimeMillis(), expiry.getExpiryForAccess(key, () -> value));
      assertThat(offHeapStore.flush(key, new DelegatingValueHolder<>(firstValueHolder)), is(false));
      assertThat(offHeapStore.flush(key, new DelegatingValueHolder<>(secondValueHolder)), is(true));
      timeSource.advanceTime(10); // this should NOT affect
      assertThat(offHeapStore.getAndFault(key).lastAccessTime(), is(secondValueHolder.creationTime() + 20));
    } finally {
      destroyStore(offHeapStore);
    }
  }

  @Test
  public void testExpiryEventFiredOnExpiredCachedEntry() throws StoreAccessException {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));

    final List<String> expiredKeys = new ArrayList<>();
    offHeapStore.getStoreEventSource().addEventListener(event -> {
      if (event.getType() == EventType.EXPIRED) {
        expiredKeys.add(event.getKey());
      }
    });

    offHeapStore.put("key1", "value1");
    offHeapStore.put("key2", "value2");

    offHeapStore.get("key1");   // Bring the entry to the caching tier

    timeSource.advanceTime(11);   // Expire the elements

    offHeapStore.get("key1");
    offHeapStore.get("key2");
    assertThat(expiredKeys, containsInAnyOrder("key1", "key2"));
    assertThat(getExpirationStatistic(offHeapStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(2L));
  }

  @Test
  public void testGetWithExpiryOnAccess() throws Exception {
    offHeapStore = createAndInitStore(timeSource, expiry().access(Duration.ZERO).build());
    offHeapStore.put("key", "value");
    final AtomicReference<String> expired = new AtomicReference<>();
    offHeapStore.getStoreEventSource().addEventListener(event -> {
      if (event.getType() == EventType.EXPIRED) {
        expired.set(event.getKey());
      }
    });
    assertThat(offHeapStore.get("key"), valueHeld("value"));
    assertThat(expired.get(), is("key"));
  }

  @Test
  public void testExpiryCreateException() throws Exception {
    offHeapStore = createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
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
    offHeapStore.put("key", "value");
    assertNull(offHeapStore.get("key"));
  }

  @Test
  public void testExpiryAccessException() throws Exception {
    offHeapStore = createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
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

    offHeapStore.put("key", "value");
    assertThat(offHeapStore.get("key"), valueHeld("value"));
    assertNull(offHeapStore.get("key"));
  }

  @Test
  public void testExpiryUpdateException() throws Exception {
    offHeapStore = createAndInitStore(timeSource, new ExpiryPolicy<String, String>() {
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

    offHeapStore.put("key", "value");
    assertThat(offHeapStore.get("key").get(), is("value"));
    timeSource.advanceTime(1000);
    offHeapStore.put("key", "newValue");
    assertNull(offHeapStore.get("key"));
  }

  @Test
  public void testGetAndFaultOnExpiredEntry() throws StoreAccessException {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));
    try {
      offHeapStore.put("key", "value");
      timeSource.advanceTime(20L);

      Store.ValueHolder<String> valueHolder = offHeapStore.getAndFault("key");
      assertThat(valueHolder, nullValue());
      assertThat(getExpirationStatistic(offHeapStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
    } finally {
      destroyStore(offHeapStore);
    }
  }

  @Test
  public void testComputeExpiresOnAccess() throws StoreAccessException {
    timeSource.advanceTime(1000L);
    offHeapStore = createAndInitStore(timeSource,
      expiry().access(Duration.ZERO).update(Duration.ZERO).build());

    offHeapStore.put("key", "value");
    Store.ValueHolder<String> result = offHeapStore.computeAndGet("key", (s, s2) -> s2, () -> false, () -> false);

    assertThat(result, valueHeld("value"));
  }

  @Test
  public void testComputeExpiresOnUpdate() throws StoreAccessException {
    timeSource.advanceTime(1000L);

    offHeapStore = createAndInitStore(timeSource,
      expiry().access(Duration.ZERO).update(Duration.ZERO).build());

    offHeapStore.put("key", "value");
    Store.ValueHolder<String> result = offHeapStore.computeAndGet("key", (s, s2) -> "newValue", () -> false, () -> false);

    assertThat(result, valueHeld("newValue"));
  }

  @Test
  public void testComputeOnExpiredEntry() throws StoreAccessException {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));

    offHeapStore.put("key", "value");
    timeSource.advanceTime(20L);

    offHeapStore.getAndCompute("key", (mappedKey, mappedValue) -> {
      assertThat(mappedKey, is("key"));
      assertThat(mappedValue, Matchers.nullValue());
      return "value2";
    });
    assertThat(getExpirationStatistic(offHeapStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testComputeIfAbsentOnExpiredEntry() throws StoreAccessException {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofMillis(10L)));

    offHeapStore.put("key", "value");
    timeSource.advanceTime(20L);

    offHeapStore.computeIfAbsent("key", mappedKey -> {
      assertThat(mappedKey, is("key"));
      return "value2";
    });
    assertThat(getExpirationStatistic(offHeapStore).count(StoreOperationOutcomes.ExpirationOutcome.SUCCESS), is(1L));
  }

  @Test
  public void testIteratorDoesNotSkipOrExpiresEntries() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));

    offHeapStore.put("key1", "value1");
    offHeapStore.put("key2", "value2");

    timeSource.advanceTime(11L);

    offHeapStore.put("key3", "value3");
    offHeapStore.put("key4", "value4");

    final List<String> expiredKeys = new ArrayList<>();
    offHeapStore.getStoreEventSource().addEventListener(event -> {
      if (event.getType() == EventType.EXPIRED) {
        expiredKeys.add(event.getKey());
      }
    });

    List<String> iteratedKeys = new ArrayList<>();
    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = offHeapStore.iterator();
    while(iterator.hasNext()) {
      iteratedKeys.add(iterator.next().getKey());
    }

    assertThat(iteratedKeys, containsInAnyOrder("key1", "key2", "key3", "key4"));
    assertThat(expiredKeys.isEmpty(), is(true));
  }

  @Test
  public void testIteratorWithSingleExpiredEntry() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));

    offHeapStore.put("key1", "value1");

    timeSource.advanceTime(11L);

    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = offHeapStore.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next().getKey(), equalTo("key1"));
    assertFalse(iterator.hasNext());
  }

  @Test
  public void testIteratorWithSingleNonExpiredEntry() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));

    offHeapStore.put("key1", "value1");

    timeSource.advanceTime(5L);

    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = offHeapStore.iterator();
    assertTrue(iterator.hasNext());
    assertThat(iterator.next().getKey(), is("key1"));
  }

  @Test
  public void testIteratorOnEmptyStore() throws Exception {
    offHeapStore = createAndInitStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10L)));

    Store.Iterator<Cache.Entry<String, Store.ValueHolder<String>>> iterator = offHeapStore.iterator();
    assertFalse(iterator.hasNext());
  }

  protected abstract AbstractOffHeapStore<String, String> createAndInitStore(final TimeSource timeSource, final ExpiryPolicy<? super String, ? super String> expiry);

  protected abstract AbstractOffHeapStore<String, byte[]> createAndInitStore(final TimeSource timeSource, final ExpiryPolicy<? super String, ? super byte[]> expiry, EvictionAdvisor<? super String, ? super byte[]> evictionAdvisor);

  protected abstract void destroyStore(AbstractOffHeapStore<?, ?> store);

  private void performEvictionTest(TestTimeSource timeSource, ExpiryPolicy<Object, Object> expiry, EvictionAdvisor<String, byte[]> evictionAdvisor) throws StoreAccessException {
    AbstractOffHeapStore<String, byte[]> offHeapStore = createAndInitStore(timeSource, expiry, evictionAdvisor);
    try {
      @SuppressWarnings("unchecked")
      StoreEventListener<String, byte[]> listener = mock(StoreEventListener.class);
      offHeapStore.getStoreEventSource().addEventListener(listener);

      byte[] value = getBytes(MemoryUnit.KB.toBytes(200));
      offHeapStore.put("key1", value);
      offHeapStore.put("key2", value);
      offHeapStore.put("key3", value);
      offHeapStore.put("key4", value);
      offHeapStore.put("key5", value);
      offHeapStore.put("key6", value);

      Matcher<StoreEvent<String, byte[]>> matcher = eventType(EventType.EVICTED);
      verify(listener, atLeast(1)).onEvent(argThat(matcher));
    } finally {
      destroyStore(offHeapStore);
    }
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
            org.terracotta.context.query.Matchers.allOf(org.terracotta.context.query.Matchers.identifier(org.terracotta.context.query.Matchers
                .subclassOf(OperationStatistic.class)),
                org.terracotta.context.query.Matchers.attributes(org.terracotta.context.query.Matchers.hasAttribute("name", "expiration")))))
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
