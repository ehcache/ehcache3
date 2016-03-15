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
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.event.EventType;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.core.exceptions.LimitExceededException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
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
import org.ehcache.core.spi.cache.Store;
import org.ehcache.core.spi.cache.events.StoreEvent;
import org.ehcache.core.spi.cache.events.StoreEventListener;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.sizeof.SizeOfEngine;
import org.hamcrest.Matcher;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.internal.store.StoreCreationEventListenerTest.eventType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * @author Abhilash
 *
 */
public class ByteAccountingTest {

  private static final Copier DEFAULT_COPIER = new IdentityCopier();
  private static final SizeOfEngine SIZE_OF_ENGINE = new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE);

  private static final String KEY = "key";
  private static final String VALUE = "value";
  private static final long SIZE_OF_KEY_VALUE_PAIR = getSize(KEY, VALUE);

  private static final SizeOfFilterSource FILTERSOURCE = new SizeOfFilterSource(true);
  private static final SizeOf SIZEOF = SizeOf.newInstance(FILTERSOURCE.getFilters());


  <K, V> OnHeapStoreForTests<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), Eviction.none());
  }

  <K, V> OnHeapStoreForTests<K, V> newStore(int capacity) {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), Eviction.none(), capacity);
  }

  <K, V> OnHeapStoreForTests<K, V> newStore(TimeSource timeSource, Expiry<? super K, ? super V> expiry) {
    return newStore(timeSource, expiry, Eviction.none());
  }

  <K, V> OnHeapStoreForTests<K, V> newStore(TimeSource timeSource, Expiry<? super K, ? super V> expiry, EvictionVeto<? super K, ? super V> veto) {
    return newStore(timeSource, expiry, veto, 100);
  }

  private <K, V> OnHeapStoreForTests<K, V> newStore(final TimeSource timeSource, final Expiry<? super K, ? super V> expiry, final EvictionVeto<? super K, ? super V> veto,
      final int capacity) {

    return new OnHeapStoreForTests<K, V>(new Store.Configuration<K, V>() {
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
      public EvictionVeto<? super K, ? super V> getEvictionVeto() {
        return veto;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super K, ? super V> getExpiry() {
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
      public int getOrderedEventParallelism() {
        return 0;
      }
    }, timeSource, new DefaultSizeOfEngine(Long.MAX_VALUE, Long.MAX_VALUE), new TestStoreEventDispatcher<K, V>());
  }

  @Test
  public void testPut() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
  }

  @Test
  public void testPutUpdate() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    String otherValue = "otherValue";
    store.put(KEY, otherValue);

    long delta = SIZEOF.deepSizeOf(otherValue) - SIZEOF.deepSizeOf(VALUE);

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR + delta));
  }

  @Test
  public void testPutExpiryOnUpdate() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource(1000L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForUpdate(String key, String oldValue, String newValue) {
        return Duration.ZERO;
      }
    });

    store.put(KEY, VALUE);
    store.put(KEY, "otherValue");

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testRemove() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeRemove = store.getCurrentUsageInBytes();
    store.remove("Another Key");
    assertThat(store.getCurrentUsageInBytes(), is(beforeRemove));
    store.remove(KEY);
    assertThat(store.getCurrentUsageInBytes(), is(0l));
  }

  @Test
  public void testRemoveTwoArg() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    long beforeRemove = store.getCurrentUsageInBytes();
    store.remove(KEY, "Another value");
    assertThat(store.getCurrentUsageInBytes(), is(beforeRemove));
    store.remove(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(0l));
  }

  @Test
  public void testReplace() throws CacheAccessException {
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
  public void testReplaceThreeArg() throws CacheAccessException {
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
  public void testPutIfAbsent() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.putIfAbsent(KEY, VALUE);
    long curent = 0l;
    assertThat(curent = store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    store.putIfAbsent(KEY, "New Value to Put");
    assertThat(store.getCurrentUsageInBytes(), is(curent));
  }

  @Test
  public void testComputeRemove() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    store.compute("another", new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return null;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return null;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(0l));
  }

  @Test
  public void testCompute() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return VALUE;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    final String replace = "Replace the original value";
    long delta = SIZEOF.deepSizeOf(replace) - SIZEOF.deepSizeOf(VALUE);

    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return replace;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR + delta));
  }

  @Test
  public void testComputeExpiryOnAccess() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(String key, String value) {
        return Duration.ZERO;
      }

      @Override
      public Duration getExpiryForUpdate(String key, String oldValue, String newValue) {
        return Duration.FOREVER;
      }
    });

    store.put(KEY, VALUE);
    store.compute(KEY, new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return s2;
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return false;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testComputeExpiryOnUpdate() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForUpdate(String key, String oldValue, String newValue) {
        return Duration.ZERO;
      }
    });

    store.put(KEY, VALUE);
    store.compute(KEY, new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return s2;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testComputeIfAbsent() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.computeIfAbsent(KEY, new Function<String, String>() {

      @Override
      public String apply(String a) {
        return VALUE;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    store.computeIfAbsent(KEY, new Function<String, String>() {

      @Override
      public String apply(String a) {
        return "Should not be replaced";
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

  }

  @Test
  public void testComputeIfAbsentExpiryOnAccess() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(String key, String value) {
        return Duration.ZERO;
      }

      @Override
      public Duration getExpiryForUpdate(String key, String oldValue, String newValue) {
        return Duration.FOREVER;
      }
    });

    store.put(KEY, VALUE);
    store.computeIfAbsent(KEY, new Function<String, String>() {
      @Override
      public String apply(String s) {
        fail("should not be called");
        return s;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testComputeIfPresentRemove() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.put(KEY, VALUE);

    store.computeIfPresent("another", new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return null;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    store.computeIfPresent(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return null;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(0l));

  }

  @Test
  public void testComputeIfPresent() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore();

    store.computeIfPresent(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return VALUE;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(0l));

    store.put(KEY, VALUE);

    store.computeIfPresent(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return VALUE;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    final String replace = "Replace the original value";
    long delta = SIZEOF.deepSizeOf(replace) - SIZEOF.deepSizeOf(VALUE);

    store.compute(KEY, new BiFunction<String, String, String>() {

      @Override
      public String apply(String a, String b) {
        return replace;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR + delta));

  }

  @Test
  public void testComputeIfPresentExpiryOnAccess() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(String key, String value) {
        return Duration.ZERO;
      }

      @Override
      public Duration getExpiryForUpdate(String key, String oldValue, String newValue) {
        return Duration.FOREVER;
      }
    });

    store.put(KEY, VALUE);
    store.computeIfPresent(KEY, new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return s2;
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return false;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testComputeIfPresentExpiryOnUpdate() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource(100L);
    OnHeapStoreForTests<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForUpdate(String key, String oldValue, String newValue) {
        return Duration.ZERO;
      }
    });

    store.put(KEY, VALUE);
    store.computeIfPresent(KEY, new BiFunction<String, String, String>() {
      @Override
      public String apply(String s, String s2) {
        return s2;
      }
    });

    assertThat(store.getCurrentUsageInBytes(), is(0L));
  }

  @Test
  public void testExpiry() throws CacheAccessException {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStoreForTests<String, String> store = newStore(timeSource, Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));

    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    timeSource.advanceTime(1);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    assertThat(store.get(KEY), nullValue());
    assertThat(store.getCurrentUsageInBytes(), is(0l));
  }

  @Test
  public void testEviction() throws CacheAccessException {
    OnHeapStoreForTests<String, String> store = newStore(1);
    StoreEventListener listener = mock(StoreEventListener.class);
    store.getStoreEventSource().addEventListener(listener);

    store.put(KEY, VALUE);
    assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));

    String key1 = "key1";
    String value1 = new String(new byte[250]);

    long requiredSize = getSize(key1, value1);

    store.put(key1, value1);
    Matcher<StoreEvent<String, byte[]>> matcher = eventType(EventType.EVICTED);
    verify(listener, times(1)).onEvent(argThat(matcher));
    if (store.get(key1) != null) {
      assertThat(store.getCurrentUsageInBytes(), is(requiredSize));
    } else {
      assertThat(store.getCurrentUsageInBytes(), is(SIZE_OF_KEY_VALUE_PAIR));
    }

  }

  static long getSize(String key, String value) {
    CopiedOnHeapValueHolder<String> valueHolder = new CopiedOnHeapValueHolder<String>(value, 0l, 0l, true, DEFAULT_COPIER);
    long size = 0L;
    try {
      size = SIZE_OF_ENGINE.sizeof(key, valueHolder);
    } catch (LimitExceededException e) {
      fail();
    }
    return size;
  }

  public static class OnHeapStoreForTests<K, V> extends OnHeapStore<K, V> {

    private static final Copier DEFAULT_COPIER = new IdentityCopier();

    public OnHeapStoreForTests(final Configuration<K, V> config, final TimeSource timeSource,
                               final SizeOfEngine engine, StoreEventDispatcher<K, V> eventDispatcher) {
      super(config, timeSource, DEFAULT_COPIER, DEFAULT_COPIER, engine, eventDispatcher);
    }

    @Override
    public long getCurrentUsageInBytes() {
      return super.getCurrentUsageInBytes();
    }

  }
}
