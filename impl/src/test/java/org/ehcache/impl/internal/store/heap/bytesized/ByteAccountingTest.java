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
package org.ehcache.impl.internal.store.heap.bytesized;

import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.event.EventType;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.heap.LimitExceededException;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.internal.events.TestStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.DefaultSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.sizeof.SizeOf;
import org.ehcache.sizeof.SizeOfFilterSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.events.StoreEvent;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.store.heap.SizeOfEngine;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.time.Duration;
import java.util.Arrays;

import static org.ehcache.config.builders.ExpiryPolicyBuilder.expiry;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.internal.store.StoreCreationEventListenerTest.eventType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * @author Abhilash
 *
 */
public class ByteAccountingTest {

  private static final SizeOfEngine SIZE_OF_ENGINE = new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE);

  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final long SIZE_OF_KEY_VALUE_PAIR = getSize(KEY, VALUE);

  private static final SizeOfFilterSource FILTERSOURCE = new SizeOfFilterSource(true);
  private static final SizeOf SIZEOF = SizeOf.newInstance(FILTERSOURCE.getFilters());


  <K, V> OnHeapStoreForTests<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, ExpiryPolicyBuilder.noExpiration(), Eviction.noAdvice());
  }

  <K, V> OnHeapStoreForTests<K, V> newStore(int capacity) {
    return newStore(SystemTimeSource.INSTANCE, ExpiryPolicyBuilder.noExpiration(), Eviction.noAdvice(), capacity);
  }

  <K, V> OnHeapStoreForTests<K, V> newStore(TimeSource timeSource, ExpiryPolicy<? super K, ? super V> expiry) {
    return newStore(timeSource, expiry, Eviction.noAdvice());
  }

  <K, V> OnHeapStoreForTests<K, V> newStore(TimeSource timeSource, ExpiryPolicy<? super K, ? super V> expiry, EvictionAdvisor<? super K, ? super V> evictionAdvisor) {
    return newStore(timeSource, expiry, evictionAdvisor, 100);
  }

  private <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource, final ExpiryPolicy<? super K, ? super V> expiry, final EvictionAdvisor<? super K, ? super V> evictionAdvisor,
      final int capacity) {

    return new OnHeapStoreForTests<>(new Store.Configuration<K, V>() {
      @SuppressWarnings("unchecked")
      @Override
      public Class<K> getKeyType() {
        return (Class<K>) String.class;
      }

      @SuppressWarnings("unchecked")
      @Override
      public Class<V> getValueType() {
        return (Class<V>) String.class;
      }

      @Override
      public EvictionAdvisor<? super K, ? super V> getEvictionAdvisor() {
        return evictionAdvisor;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public ExpiryPolicy<? super K, ? super V> getExpiry() {
        return expiry;
      }

      @Override
      public ResourcePools getResourcePools() {
        return newResourcePoolsBuilder().heap(capacity, MemoryUnit.KB).build();
      }

      @Override
      public Serializer<K> getKeySerializer() {
        throw new AssertionError("By-ref heap store using serializers!");
      }

      @Override
      public Serializer<V> getValueSerializer() {
        throw new AssertionError("By-ref heap store using serializers!");
      }

      @Override
      public int getDispatcherConcurrency() {
        return 0;
      }

      @Override
      public CacheLoaderWriter<? super K, V> getCacheLoaderWriter() {
        return null;
      }
    }, timeSource, new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE), new TestStoreEventDispatcher<>());
  }

  @Test
  public void testPut() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
  }

  @Test
  public void testPutUpdate() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    String otherValue = "otherValue";
    store.put(KEY, otherValue);

    long delta = SIZEOF.deepSizeOf(otherValue) - SIZEOF.deepSizeOf(VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR + delta));
  }

  @Test
  public void testPutExpiryOnCreate() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().create(Duration.ZERO).build());

    store.put(KEY, VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testPutExpiryOnUpdate() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().update(Duration.ZERO).build());

    store.put(KEY, VALUE);
    store.put(KEY, "otherValue");

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testRemove() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeRemove = store.getCurrentUsageInBytes();
    store.remove("Another Key");
    assertThat(store.getCurrentUsageInBytes(), is(beforeRemove));
    store.remove(KEY);
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testRemoveExpired() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, ttlCreation600ms());

    store.put(KEY, VALUE);
    timeSource.advanceTime(1000L);
    store.remove(KEY);
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testRemoveTwoArg() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeRemove = store.getCurrentUsageInBytes();
    store.remove(KEY, "Another value");
    assertThat(store.getCurrentUsageInBytes(), is(beforeRemove));
    store.remove(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testRemoveTwoArgExpired() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, ttlCreation600ms());

    store.put(KEY, VALUE);
    timeSource.advanceTime(1000L);
    store.remove(KEY, "whatever value, it is expired anyway");
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testRemoveTwoArgExpiresOnAccess() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().access(Duration.ZERO).build());

    store.put(KEY, VALUE);
    store.remove(KEY, "whatever value, it expires on access");
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testReplace() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeReplace = store.getCurrentUsageInBytes();
    store.replace("Another Key", "Another Value");
    assertThat(store.getCurrentUsageInBytes(), is(beforeReplace));

    String toReplace = "Replaced Value";
    store.replace(KEY, toReplace);
    long delta = SIZEOF.deepSizeOf(toReplace) - SIZEOF.deepSizeOf(VALUE);
    long afterReplace = store.getCurrentUsageInBytes();

    assertThat(afterReplace - beforeReplace, is(delta));

    //when delta is negative
    store.replace(KEY, VALUE);
    assertThat(afterReplace - store.getCurrentUsageInBytes(), is(delta));
  }

  @Test
  public void testReplaceTwoArgExpired() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, ttlCreation600ms());

    store.put(KEY, VALUE);
    timeSource.advanceTime(1000L);
    store.replace(KEY, "whatever value, it is expired anyway");
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testReplaceTwoArgExpiresOnUpdate() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().update(Duration.ZERO).build());

    store.put(KEY, VALUE);
    store.replace(KEY, "whatever value, it expires on update");
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testReplaceThreeArg() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeReplace = store.getCurrentUsageInBytes();
    store.replace(KEY, "Another Value", VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(beforeReplace));

    String toReplace = "Replaced Value";
    store.replace(KEY, VALUE, toReplace);
    long delta = SIZEOF.deepSizeOf(toReplace) - SIZEOF.deepSizeOf(VALUE);
    long afterReplace = store.getCurrentUsageInBytes();

    assertThat(afterReplace - beforeReplace, is(delta));

    //when delta is negative
    store.replace(KEY, toReplace, VALUE);
    assertThat(afterReplace - store.getCurrentUsageInBytes(), is(delta));
  }

  @Test
  public void testReplaceThreeArgExpired() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, ttlCreation600ms());

    store.put(KEY, VALUE);
    timeSource.advanceTime(1000L);
    store.replace(KEY, VALUE, "whatever value, it is expired anyway");
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testReplaceThreeArgExpiresOnUpdate() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().update(Duration.ZERO).build());

    store.put(KEY, VALUE);
    store.replace(KEY, VALUE, "whatever value, it expires on update");
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testPutIfAbsent() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.putIfAbsent(KEY, VALUE, b -> {});
    long current = store.getCurrentUsageInBytes();
    assertThat(current, is(SIZE_OF_KEY_VALUE_PAIR));

    store.putIfAbsent(KEY, "New Value to Put", b -> {});
    assertThat(store.getCurrentUsageInBytes(), is(current));
  }

  @Test
  public void testPutIfAbsentOverExpired() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, ttlCreation600ms());

    store.put(KEY, "an expired value");
    timeSource.advanceTime(1000L);
    store.putIfAbsent(KEY, VALUE, b -> {});
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
  }

  @Test
  public void testPutIfAbsentExpiresOnAccess() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().access(Duration.ZERO).build());

    store.put(KEY, VALUE);
    store.putIfAbsent(KEY, "another value ... whatever", b -> {});
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testInvalidate() throws StoreAccessException {
    OnHeapStoreForTests<Object, Object> store = newStore();
    store.put(KEY, VALUE);
    store.invalidate(KEY);

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testSilentInvalidate() throws StoreAccessException {
    OnHeapStoreForTests<Object, Object> store = newStore();
    store.put(KEY, VALUE);
    store.silentInvalidate(KEY, objectValueHolder -> {
      // Nothing to do
      return null;
    });

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testComputeRemove() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    store.getAndCompute("another", (a, b) -> null);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    store.getAndCompute(KEY, (a, b) -> null);

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testCompute() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.getAndCompute(KEY, (a, b) -> VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    final String replace = "Replace the original value";
    long delta = SIZEOF.deepSizeOf(replace) - SIZEOF.deepSizeOf(VALUE);

    store.getAndCompute(KEY, (a, b) -> replace);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR + delta));
  }

  @Test
  public void testComputeExpiryOnAccess() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().access(Duration.ZERO).build());

    store.put(KEY, VALUE);
    store.computeAndGet(KEY, (s, s2) -> s2, () -> false, () -> false);

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testGetAndComputeExpiryOnUpdate() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().update(Duration.ZERO).build());

    store.put(KEY, VALUE);
    store.getAndCompute(KEY, (s, s2) -> s2);

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testComputeIfAbsent() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.computeIfAbsent(KEY, a -> VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    store.computeIfAbsent(KEY, a -> "Should not be replaced");

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
  }

  @Test
  public void testComputeIfAbsentExpireOnCreate() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().create(Duration.ZERO).build());

    store.computeIfAbsent(KEY, s -> VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testComputeIfAbsentExpiryOnAccess() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, expiry().access(Duration.ZERO).build());

    store.put(KEY, VALUE);
    store.computeIfAbsent(KEY, s -> {
      fail("should not be called");
      return s;
    });

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testExpiry() throws StoreAccessException {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStoreForTests<String, String> store = newStore(timeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));

    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    timeSource.advanceTime(1);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    assertThat(store.get(KEY), nullValue());
    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testEviction() throws StoreAccessException {
    OnHeapStoreForTests<String, String> store = newStore(1);
    @SuppressWarnings("unchecked")
    StoreEventListener<String, String> listener = mock(StoreEventListener.class);
    store.getStoreEventSource().addEventListener(listener);

    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    String key1 = "key1";
    char[] chars = new char[250];
    Arrays.fill(chars, (char) 0xffff);
    String value1 = new String(chars);

    long requiredSize = getSize(key1, value1);

    store.put(key1, value1);
    Matcher<StoreEvent<String, String>> matcher = eventType(EventType.EVICTED);
    verify(listener, times(1)).onEvent(argThat(matcher));
    if (store.get(key1) != null) {
      assertThat(store.getCurrentUsageInBytes(), is(requiredSize));
    } else {
      assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    }

  }

  private ExpiryPolicy<Object, Object> ttlCreation600ms() {
    return expiry().create(Duration.ofMillis(600L)).build();
  }

  static long getSize(String key, String value) {
    CopiedOnHeapValueHolder<String> valueHolder = new CopiedOnHeapValueHolder<>(value, 0L, 0L, true, IdentityCopier.identityCopier());
    long size = 0L;
    try {
      size = SIZE_OF_ENGINE.sizeof(key, valueHolder);
    } catch (LimitExceededException e) {
      fail();
    }
    return size;
  }

  static class OnHeapStoreForTests<K, V> extends OnHeapStore<K, V> {

    @SuppressWarnings("unchecked")
    OnHeapStoreForTests(final Configuration<K, V> config, final TimeSource timeSource,
                        final SizeOfEngine engine, StoreEventDispatcher<K, V> eventDispatcher) {
      super(config, timeSource, IdentityCopier.identityCopier(), IdentityCopier.identityCopier(), engine, eventDispatcher);
    }

    long getCurrentUsageInBytes() {
      return super.byteSized();
    }

  }
}
