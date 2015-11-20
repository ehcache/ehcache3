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
package org.ehcache.internal.store.heap;

import org.ehcache.Cache;
import org.ehcache.Cache.Entry;
import org.ehcache.CacheConfigurationChangeEvent;
import org.ehcache.CacheConfigurationChangeListener;
import org.ehcache.CacheConfigurationProperty;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.events.StoreEventListener;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.NullaryFunction;
import org.ehcache.internal.TimeSource;
import org.ehcache.internal.copy.IdentityCopier;
import org.ehcache.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.internal.util.StatisticsTestUtils;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.cache.Store.Iterator;
import org.ehcache.spi.cache.Store.ValueHolder;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.statistics.CachingTierOperationOutcomes;
import org.ehcache.statistics.StoreOperationOutcomes;
import org.junit.Test;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public abstract class BaseOnHeapStoreTest {

  private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException();

  @Test
  public void testEvictEmptyStoreDoesNothing() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventListener<String, String> listener = addListener(store);
    assertThat(store.evict(), is(false));
    verify(listener, never()).onEviction(Matchers.<String>any(), Matchers.<Store.ValueHolder<String>>any());
  }

  @Test
  public void testEvictWithNoVetoDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventListener<String, String> listener = addListener(store);
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(), is(true));
    assertThat(storeSize(store), is(99));
    verify(listener, times(1)).onEviction(Matchers.<String>any(), Matchers.<Store.ValueHolder<String>>any());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
  }

  @Test
  public void testEvictWithFullVetoDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore(new EvictionVeto<String, String>() {
      @Override
      public boolean test(Entry<String, String> argument) {
        return true;
      }
    });
    StoreEventListener<String, String> listener = addListener(store);
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(), is(true));
    assertThat(storeSize(store), is(99));
    verify(listener, times(1)).onEviction(Matchers.<String>any(), Matchers.<Store.ValueHolder<String>>any());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
  }

  @Test
  public void testEvictWithBrokenVetoDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore(new EvictionVeto<String, String>() {
      @Override
      public boolean test(Entry<String, String> argument) {
        throw new UnsupportedOperationException("Broken veto!");
      }
    });
    StoreEventListener<String, String> listener = addListener(store);
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(), is(true));
    assertThat(storeSize(store), is(99));
    verify(listener, times(1)).onEviction(Matchers.<String>any(), Matchers.<Store.ValueHolder<String>>any());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
  }

  @Test
  public void testEvictionPrioritizationIsObserved() throws Exception {

    EvictionPrioritizer<String, String> prioritizer = new EvictionPrioritizer<String, String>() {

      @Override
      public int compare(Entry<String, String> o1, Entry<String, String> o2) {
        return Integer.decode(o1.getValue()).compareTo(Integer.decode(o2.getValue()));
      }
    };
    OnHeapStore<String, String> store = newStore(OnHeapStore.SAMPLE_SIZE + 1, prioritizer);

    StoreEventListener<String, String> listener = addListener(store);

    for (int i = 0; i < OnHeapStore.SAMPLE_SIZE; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }

    assertThat(store.evict(), is(true));
    verify(listener, times(1)).onEviction(eq(Integer.toString(OnHeapStore.SAMPLE_SIZE - 1)), any(ValueHolder.class));
  }

  @Test
  public void testBrokenEvictionPrioritizationStillEvicts() throws Exception {
    EvictionPrioritizer<String, String> prioritizer = new EvictionPrioritizer<String, String>() {

      @Override
      public int compare(Entry<String, String> o1, Entry<String, String> o2) {
        throw new UnsupportedOperationException("Broken prioritizer!");
      }
    };
    OnHeapStore<String, String> store = newStore(OnHeapStore.SAMPLE_SIZE + 1, prioritizer);

    StoreEventListener<String, String> listener = addListener(store);

    for (int i = 0; i < OnHeapStore.SAMPLE_SIZE; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }

    assertThat(store.evict(), is(true));
    verify(listener, times(1)).onEviction(any(String.class), any(ValueHolder.class));
  }

  @Test
  public void testGet() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.HIT));
  }

  @Test
  public void testGetNoPut() throws Exception {
    OnHeapStore<String, String> store = newStore();
    assertThat(store.get("key"), nullValue());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS));
  }

  @Test
  public void testGetExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    timeSource.advanceTime(1);
    assertThat(store.get("key"), nullValue());
    checkExpiryEvent(listener, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.HIT, StoreOperationOutcomes.GetOutcome.MISS));
  }

  @Test
  public void testGetNoExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(2, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);
    store.put("key", "value");
    timeSource.advanceTime(1);
    assertThat(store.get("key").value(), equalTo("value"));
    verify(listener, never()).onExpiration(Matchers.<String>any(), Matchers.<Store.ValueHolder<String>>any());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.HIT));
  }

  @Test
  public void testAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());
    store.put("key", "value");

    long first = store.get("key").lastAccessTime(TimeUnit.MILLISECONDS);
    assertThat(first, equalTo(timeSource.getTimeMillis()));
    final long advance = 5;
    timeSource.advanceTime(advance);
    long next = store.get("key").lastAccessTime(TimeUnit.MILLISECONDS);
    assertThat(next, equalTo(first + advance));
  }

  @Test
  public void testContainsKey() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    assertThat(store.containsKey("key"), is(true));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.HIT));
  }

  @Test
  public void testNotContainsKey() throws Exception {
    OnHeapStore<String, String> store = newStore();
    assertThat(store.containsKey("key"), is(false));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS));
  }

  @Test
  public void testContainsKeyExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);

    store.put("key", "value");
    timeSource.advanceTime(1);
    assertThat(store.containsKey("key"), is(false));
    checkExpiryEvent(listener, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.MISS));
  }

  @Test
  public void testPut() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testPutOverwrite() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    store.put("key", "value2");
    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testCreateTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());
    assertThat(store.containsKey("key"), is(false));
    store.put("key", "value");
    ValueHolder<String> valueHolder = store.get("key");
    assertThat(timeSource.getTimeMillis(), equalTo(valueHolder.creationTime(TimeUnit.MILLISECONDS)));
  }

  @Test
  public void testInvalidate() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    store.invalidate("key");
    assertThat(store.get("key"), nullValue());
    StatisticsTestUtils.validateStats(store, EnumSet.of(CachingTierOperationOutcomes.InvalidateOutcome.REMOVED));
  }

  @Test
  public void testPutIfAbsentNoValue() throws Exception {
    OnHeapStore<String, String> store = newStore();
    ValueHolder<String> prev = store.putIfAbsent("key", "value");
    assertThat(prev, nullValue());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testPutIfAbsentValuePresent() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    ValueHolder<String> prev = store.putIfAbsent("key", "value2");
    assertThat(prev.value(), equalTo("value"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  @Test
  public void testPutIfAbsentUpdatesAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());
    assertThat(store.get("key"), nullValue());
    store.putIfAbsent("key", "value");
    long first = store.get("key").lastAccessTime(TimeUnit.MILLISECONDS);
    timeSource.advanceTime(1);
    long next = store.putIfAbsent("key", "value2").lastAccessTime(TimeUnit.MILLISECONDS);
    assertThat(next - first, equalTo(1L));
  }

  @Test
  public void testPutIfAbsentExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);
    store.put("key", "value");
    timeSource.advanceTime(1);
    ValueHolder<String> prev = store.putIfAbsent("key", "value2");
    assertThat(prev, nullValue());
    assertThat(store.get("key").value(), equalTo("value2"));
    checkExpiryEvent(listener, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testRemoveTwoArgMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    boolean removed = store.remove("key", "value");
    assertThat(removed, equalTo(true));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED));
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testRemoveTwoArgNoMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    boolean removed = store.remove("key", "not value");
    assertThat(removed, equalTo(false));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testRemoveTwoArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    timeSource.advanceTime(1);
    boolean removed = store.remove("key", "value");
    assertThat(removed, equalTo(false));
    checkExpiryEvent(listener, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testReplaceTwoArgPresent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    ValueHolder<String> existing = store.replace("key", "value2");
    assertThat(existing.value(), equalTo("value"));
    assertThat(store.get("key").value(), equalTo("value2"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.REPLACED));
  }

  @Test
  public void testReplaceTwoArgAbsent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    ValueHolder<String> existing = store.replace("key", "value");
    assertThat(existing, nullValue());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.MISS));
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testReplaceTwoArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);

    store.put("key", "value");
    timeSource.advanceTime(1);
    ValueHolder<String> existing = store.replace("key", "value2");
    assertThat(existing, nullValue());
    assertThat(store.get("key"), nullValue());
    checkExpiryEvent(listener, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testReplaceThreeArgMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");

    boolean replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(true));
    assertThat(store.get("key").value(), equalTo("value2"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED));
  }

  @Test
  public void testReplaceThreeArgNoMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();

    boolean replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(false));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS));

    store.put("key", "value");

    replaced = store.replace("key", "not value", "value2");
    assertThat(replaced, equalTo(false));
    StatisticsTestUtils.validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.MISS, 2L);
  }

  @Test
  public void testReplaceThreeArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);

    store.put("key", "value");
    timeSource.advanceTime(1);
    boolean replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(false));
    assertThat(store.get("key"), nullValue());
    checkExpiryEvent(listener, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testIterator() throws Exception {
    OnHeapStore<String, String> store = newStore();

    Iterator<Entry<String, ValueHolder<String>>> iter = store.iterator();
    assertThat(iter.hasNext(), equalTo(false));
    try {
      iter.next();
      fail("NoSuchElementException expected");
    } catch (NoSuchElementException nse) {
      // expected
    }

    store.put("key1", "value1");
    iter = store.iterator();
    assertThat(iter.hasNext(), equalTo(true));
    assertEntry(iter.next(), "key1", "value1");
    assertThat(iter.hasNext(), equalTo(false));

    store.put("key2", "value2");
    Map<String, String> observed = observe(store.iterator());
    assertThat(2, equalTo(observed.size()));
    assertThat(observed.get("key1"), equalTo("value1"));
    assertThat(observed.get("key2"), equalTo("value2"));
  }

  @Test
  public void testIteratorExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);
    store.put("key1", "value1");
    store.put("key2", "value2");
    timeSource.advanceTime(1);
    store.put("key3", "value3");

    Map<String, String> observed = observe(store.iterator());
    assertThat(1, equalTo(observed.size()));
    assertThat(observed.get("key3"), equalTo("value3"));

    timeSource.advanceTime(1);
    observed = observe(store.iterator());
    assertThat(0, equalTo(observed.size()));
    checkExpiryEvent(listener, "key1", "value1");
    checkExpiryEvent(listener, "key2", "value2");
    checkExpiryEvent(listener, "key3", "value3");
    StatisticsTestUtils.validateStat(store, StoreOperationOutcomes.ExpirationOutcome.SUCCESS, 3L);
  }

  @Test
  public void testIteratorUpdatesAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());

    store.put("key1", "value1");
    store.put("key2", "value2");

    timeSource.advanceTime(5);

    Map<String, Long> times = observeAccessTimes(store.iterator());
    assertThat(2, equalTo(times.size()));
    assertThat(times.get("key1"), equalTo(5L));
    assertThat(times.get("key2"), equalTo(5L));
  }

  @Test
  public void testComputeReplaceTrue() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());

    store.put("key", "value");
    ValueHolder<String> installedHolder = store.get("key");
    long createTime = installedHolder.creationTime(TimeUnit.MILLISECONDS);
    long accessTime = installedHolder.lastAccessTime(TimeUnit.MILLISECONDS);
    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return mappedValue;
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return true;
      }
    });

    assertThat(newValue.value(), equalTo("value"));
    assertThat(createTime + 1, equalTo(newValue.creationTime(TimeUnit.MILLISECONDS)));
    assertThat(accessTime + 1, equalTo(newValue.lastAccessTime(TimeUnit.MILLISECONDS)));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.PUT));
  }

  @Test
  public void testComputeReplaceFalse() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());

    store.put("key", "value");
    ValueHolder<String> installedHolder = store.get("key");
    long createTime = installedHolder.creationTime(TimeUnit.MILLISECONDS);
    long accessTime = installedHolder.lastAccessTime(TimeUnit.MILLISECONDS);
    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return mappedValue;
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return false;
      }
    });

    assertThat(newValue.value(), equalTo("value"));
    assertThat(createTime, equalTo(newValue.creationTime(TimeUnit.MILLISECONDS)));
    assertThat(accessTime + 1, equalTo(newValue.lastAccessTime(TimeUnit.MILLISECONDS)));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.HIT));
  }

  @Test
  public void testCompute() throws Exception {
    OnHeapStore<String, String> store = newStore();

    ValueHolder<String> newValue = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        assertThat(mappedKey, equalTo("key"));
        assertThat(mappedValue, nullValue());
        return "value";
      }
    });

    assertThat(newValue.value(), equalTo("value"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.PUT));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeNull() throws Exception {
    OnHeapStore<String, String> store = newStore();

    ValueHolder<String> newValue = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return null;
      }
    });

    assertThat(newValue, nullValue());
    assertThat(store.get("key"), nullValue());
    StatisticsTestUtils.validateStat(store, StoreOperationOutcomes.ComputeOutcome.MISS, 1L);

    store.put("key", "value");
    newValue = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return null;
      }
    });

    assertThat(newValue, nullValue());
    assertThat(store.get("key"), nullValue());
    StatisticsTestUtils.validateStat(store, StoreOperationOutcomes.ComputeOutcome.REMOVED, 1L);
  }

  @Test
  public void testComputeException() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    try {
      store.compute("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String mappedKey, String mappedValue) {
          throw RUNTIME_EXCEPTION;
        }
      });
      fail("RuntimeException expected");
    } catch (CacheAccessException cae) {
      assertThat(cae.getCause(), is((Throwable)RUNTIME_EXCEPTION));
    }
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeExistingValue() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    ValueHolder<String> newValue = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        assertThat(mappedKey, equalTo("key"));
        assertThat(mappedValue, equalTo("value"));
        return "value2";
      }
    });

    assertThat(newValue.value(), equalTo("value2"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.PUT));
    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testComputeExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    StoreEventListener<String, String> listener = addListener(store);
    timeSource.advanceTime(1);
    ValueHolder<String> newValue = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        assertThat(mappedKey, equalTo("key"));
        assertThat(mappedValue, nullValue());
        return "value2";
      }
    });

    assertThat(newValue.value(), equalTo("value2"));
    assertThat(store.get("key").value(), equalTo("value2"));
    checkExpiryEvent(listener, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testComputeIfAbsent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    ValueHolder<String> newValue = store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        assertThat(mappedKey, equalTo("key"));
        return "value";
      }
    });

    assertThat(newValue.value(), equalTo("value"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.PUT));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeIfAbsentExisting() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    ValueHolder<String> newValue = store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        fail("Should not be called");
        return null;
      }
    });

    assertThat(newValue.value(), equalTo("value"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.HIT));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testComputeIfAbsentReturnNull() throws Exception {
    OnHeapStore<String, String> store = newStore();

    ValueHolder<String> newValue = store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        return null;
      }
    });

    assertThat(newValue, nullValue());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.NOOP));
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testComputeIfAbsentException() throws Exception {
    OnHeapStore<String, String> store = newStore();

    try {
      store.computeIfAbsent("key", new Function<String, String>() {
        @Override
        public String apply(String mappedKey) {
          throw RUNTIME_EXCEPTION;
        }
      });
      fail("Expected exception");
    } catch (CacheAccessException cae) {
      assertThat(cae.getCause(), is((Throwable)RUNTIME_EXCEPTION));
    }

    assertThat(store.get("key"), nullValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testComputeIfAbsentExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    StoreEventListener<String, String> listener = addListener(store);
    
    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.computeIfAbsent("key", new Function<String, String>() {
      @Override
      public String apply(String mappedKey) {
        assertThat(mappedKey, equalTo("key"));
        return "value2";
      }
    });

    assertThat(newValue.value(), equalTo("value2"));
    assertThat(store.get("key").value(), equalTo("value2"));
    final String value = "value";
    verify(listener).onExpiration(eq("key"), valueHolderValueEq(value));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testExpiryCreateException() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, String>() {

      @Override
      public Duration getExpiryForCreation(String key, String value) {
        throw RUNTIME_EXCEPTION;
      }

      @Override
      public Duration getExpiryForAccess(String key, String value) {
        throw new AssertionError();
      }

      @Override
      public Duration getExpiryForUpdate(String key, String oldValue, String newValue) {
        throw new AssertionError();
      }
    });

    store.put("key", "value");
    assertThat(store.containsKey("key"), equalTo(false));
  }

  @Test
  public void testExpiryAccessException() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, String>() {

      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.FOREVER;
      }

      @Override
      public Duration getExpiryForAccess(String key, String value) {
        throw RUNTIME_EXCEPTION;
      }

      @Override
      public Duration getExpiryForUpdate(String key, String oldValue, String newValue) {
        return null;
      }
    });

    store.put("key", "value");
    assertNull(store.get("key"));
  }

  @Test
  public void testExpiryUpdateException() throws Exception{
    final TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, String>() {
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
        if (timeSource.getTimeMillis() > 0) {
          throw new RuntimeException();
        }
        return Duration.FOREVER;
      }
    });

    store.put("key", "value");
    store.get("key");
    timeSource.advanceTime(1000);

    store.put("key", "newValue");
    assertNull(store.get("key"));
  }

  @Test
  public void testComputeIfPresent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    ValueHolder<String> newValue = store.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        assertThat(mappedKey, equalTo("key"));
        assertThat(mappedValue, equalTo("value"));
        return "value2";
      }
    });

    assertThat(newValue.value(), equalTo("value2"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfPresentOutcome.PUT));
    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testComputeIfPresentMissing() throws Exception {
    OnHeapStore<String, String> store = newStore();

    ValueHolder<String> newValue = store.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        fail("Should not have been called");
        return null;
      }
    });

    assertThat(newValue, nullValue());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfPresentOutcome.MISS));
  }

  @Test
  public void testComputeIfPresentReturnNull() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    ValueHolder<String> newValue = store.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        return null;
      }
    });

    assertThat(newValue, nullValue());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfPresentOutcome.REMOVED));
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testComputeIfPresentException() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    try {
      store.computeIfPresent("key", new BiFunction<String, String, String>() {
        @Override
        public String apply(String mappedKey, String mappedValue) {
          throw RUNTIME_EXCEPTION;
        }
      });
      fail("Expected exception");
    } catch (CacheAccessException cae) {
      assertThat(cae.getCause(), is((Throwable)RUNTIME_EXCEPTION));
    }

    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testComputeIfPresentExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);

    store.put("key", "value");
    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.computeIfPresent("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String mappedKey, String mappedValue) {
        throw new AssertionError();
      }
    });

    assertThat(newValue, nullValue());
    assertThat(store.get("key"), nullValue());
    verify(listener).onExpiration(eq("key"), valueHolderValueEq("value"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testGetOrComputeIfAbsentExpiresOnHit() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    StoreEventListener<String, String> listener = addListener(store);

    store.put("key", "value");
    assertThat(storeSize(store), is(1));
    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.getOrComputeIfAbsent("key", new Function<String, ValueHolder<String>>() {
      @Override
      public ValueHolder<String> apply(final String s) {
        throw new AssertionError();
      }
    });

    assertThat(newValue, nullValue());
    assertThat(store.get("key"), nullValue());
    verify(listener).onExpiration(eq("key"), valueHolderValueEq("value"));
    assertThat(storeSize(store), is(0));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testGetOrComputeIfAbsentRemovesFault() throws CacheAccessException {
    final OnHeapStore<String, String> store = newStore();
    final CountDownLatch testCompletionLatch = new CountDownLatch(1);
    final CountDownLatch threadFaultCompletionLatch = new CountDownLatch(1);
    final CountDownLatch mainFaultCreationLatch = new CountDownLatch(1);
    

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          try {
            mainFaultCreationLatch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          store.remove("1");
          store.getOrComputeIfAbsent("1", new Function<String, ValueHolder<String>>() {
            @Override
            public ValueHolder<String> apply(final String s) {
              threadFaultCompletionLatch.countDown();
              try {
                testCompletionLatch.await();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              return null;
            }
          });
        } catch (CacheAccessException e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();
      
    store.getOrComputeIfAbsent("1", new Function<String, ValueHolder<String>>() {
      @Override
      public ValueHolder<String> apply(final String s) {
        try {
          mainFaultCreationLatch.countDown();
          threadFaultCompletionLatch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        } 
        return null;
      }
    });
    testCompletionLatch.countDown();
  }

  @Test
  public void testGetOrComputeIfAbsentInvalidatesFault() throws CacheAccessException, InterruptedException {
    final OnHeapStore<String, String> store = newStore();
    final CountDownLatch testCompletionLatch = new CountDownLatch(1);
    final CountDownLatch threadFaultCompletionLatch = new CountDownLatch(1);
    CachingTier.InvalidationListener<String, String> invalidationListener = new CachingTier.InvalidationListener<String, String>() {
      @Override
      public void onInvalidation(final String key, final ValueHolder<String> valueHolder) {
        try {
          valueHolder.getId();
        } catch (Exception e) {
          e.printStackTrace();
          fail("Test tried to invalidate Fault");
        }
      }
    };

    store.setInvalidationListener(invalidationListener);
    updateStoreCapacity(store, 1);

    Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          store.getOrComputeIfAbsent("1", new Function<String, ValueHolder<String>>() {
            @Override
            public ValueHolder<String> apply(final String s) {
              threadFaultCompletionLatch.countDown();
              try {
                testCompletionLatch.await();
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
              return null;
            }
          });
        } catch (CacheAccessException e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();

    threadFaultCompletionLatch.await();
    store.getOrComputeIfAbsent("10", new Function<String, ValueHolder<String>>() {
      @Override
      public ValueHolder<String> apply(final String s) {
        return null;
      }
    });
    testCompletionLatch.countDown();
  }

  @Test
  public void testGetOrComputeIfAbsentContention() throws InterruptedException {

    final OnHeapStore<String, String> store = newStore();

    int threads = 10;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch endLatch = new CountDownLatch(threads);

    Runnable runnable = new Runnable() {

      @Override
      public void run() {
        try {
          startLatch.await();
        } catch (InterruptedException e) {
          fail("Got an exception waiting to start thread " + e);
        }
        try {
          ValueHolder<String> result = store.getOrComputeIfAbsent("42", new Function<String, ValueHolder<String>>() {
            @Override
            public ValueHolder<String> apply(String key) {
              return new CopiedOnHeapValueHolder<String>("theAnswer!", System.currentTimeMillis(), -1, new IdentityCopier<String>());
            }
          });
          assertThat(result.value(), is("theAnswer!"));
          endLatch.countDown();
        } catch (Exception e) {
          e.printStackTrace();
          fail("Got an exception " + e);
        }
      }
    };

    for (int i = 0; i < threads; i++) {
      new Thread(runnable).start();
    }

    startLatch.countDown();

    boolean result = endLatch.await(2, TimeUnit.SECONDS);
    if (!result) {
      fail("Wait expired before completion, logs should have exceptions");
    }
  }

  @Test
  public void testConcurrentFaultingAndInvalidate() throws Exception {
    final OnHeapStore<String, String> store = newStore();
    CachingTier.InvalidationListener<String, String> invalidationListener = mock(CachingTier.InvalidationListener.class);
    store.setInvalidationListener(invalidationListener);

    final AtomicReference<AssertionError> failedInThread = new AtomicReference<AssertionError>();

    final CountDownLatch getLatch = new CountDownLatch(1);
    final CountDownLatch invalidateLatch = new CountDownLatch(1);

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          store.getOrComputeIfAbsent("42", new Function<String, ValueHolder<String>>() {
            @Override
            public ValueHolder<String> apply(String aString) {
              invalidateLatch.countDown();
              try {
                boolean await = getLatch.await(5, TimeUnit.SECONDS);
                if (!await) {
                  failedInThread.set(new AssertionError("latch timed out"));
                }
              } catch (InterruptedException e) {
                failedInThread.set(new AssertionError("Interrupted exception: " + e.getMessage()));
              }
              return new CopiedOnHeapValueHolder<String>("TheAnswer!", System.currentTimeMillis(), new IdentityCopier<String>());
            }
          });
        } catch (CacheAccessException caex) {
          failedInThread.set(new AssertionError("CacheAccessException: " + caex.getMessage()));
        }
      }
    }).start();

    boolean await = invalidateLatch.await(5, TimeUnit.SECONDS);
    if (!await) {
      fail("latch timed out");
    }
    store.invalidate("42");
    getLatch.countDown();

    verify(invalidationListener, never()).onInvalidation(any(String.class), any(ValueHolder.class));
    if (failedInThread.get() != null) {
      throw failedInThread.get();
    }
  }

  @Test
  public void testConcurrentSilentFaultingAndInvalidate() throws Exception {
    final OnHeapStore<String, String> store = newStore();

    final AtomicReference<AssertionError> failedInThread = new AtomicReference<AssertionError>();

    final CountDownLatch getLatch = new CountDownLatch(1);
    final CountDownLatch invalidateLatch = new CountDownLatch(1);

    new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          store.getOrComputeIfAbsent("42", new Function<String, ValueHolder<String>>() {
            @Override
            public ValueHolder<String> apply(String aString) {
              invalidateLatch.countDown();
              try {
                boolean await = getLatch.await(5, TimeUnit.SECONDS);
                if (!await) {
                  failedInThread.set(new AssertionError("latch timed out"));
                }
              } catch (InterruptedException e) {
                failedInThread.set(new AssertionError("Interrupted exception: " + e.getMessage()));
              }
              return new CopiedOnHeapValueHolder<String>("TheAnswer!", System.currentTimeMillis(), new IdentityCopier<String>());
            }
          });
        } catch (CacheAccessException caex) {
          failedInThread.set(new AssertionError("CacheAccessException: " + caex.getMessage()));
        }
      }
    }).start();

    boolean await = invalidateLatch.await(5, TimeUnit.SECONDS);
    if (!await) {
      fail("latch timed out");
    }
    store.silentInvalidate("42", new Function<ValueHolder<String>, Void>() {
      @Override
      public Void apply(ValueHolder<String> stringValueHolder) {
        if (stringValueHolder != null) {
          assertThat("Expected a null parameter otherwise we leak a Fault", stringValueHolder, nullValue());
        }
        return null;
      }
    });
    getLatch.countDown();

    if (failedInThread.get() != null) {
      throw failedInThread.get();
    }
  }

  @Test
  public void testEvictionDoneUnderEvictedKeyLockScope() throws Exception {
    final OnHeapStore<String, String> store = newStore();

    updateStoreCapacity(store, 2);

    // Fill in store at capacity
    store.put("keyA", "valueA");
    store.put("keyB", "valueB");

    final Exchanger<String> keyExchanger = new Exchanger<String>();
    final AtomicReference<String> reference = new AtomicReference<String>();
    final CountDownLatch faultingLatch = new CountDownLatch(1);

    // prepare concurrent faulting
    final Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        String key;

        try {
          key = keyExchanger.exchange("ready to roll!");
          store.put(key, "updateValue");
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (CacheAccessException e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();

    store.setInvalidationListener(new CachingTier.InvalidationListener<String, String>() {
      @Override
      public void onInvalidation(String key, ValueHolder<String> valueHolder) {
        // Only want to exchange on the first invalidation!
        if (reference.compareAndSet(null, key)) {
          try {
            keyExchanger.exchange(key);
            long now = System.nanoTime();
            while (!thread.getState().equals(Thread.State.BLOCKED)) {
              Thread.yield();
              if (System.nanoTime() - now > TimeUnit.MILLISECONDS.toNanos(500)) {
                assertThat(thread.getState(), is(Thread.State.BLOCKED));
              }
            }
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    //trigger eviction
    store.put("keyC", "valueC");
  }

  @Test
  public void testIteratorExpiryHappensUnderExpiredKeyLockScope() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    final OnHeapStore<String, String> store = newStore(testTimeSource, Expirations.timeToLiveExpiration(new Duration(10, TimeUnit.MILLISECONDS)));

    store.put("key", "value");

    final CountDownLatch expiryLatch = new CountDownLatch(1);

    final Thread thread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          expiryLatch.await();
          store.put("key", "newValue");
        } catch (InterruptedException e) {
          e.printStackTrace();
        } catch (CacheAccessException e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();

    store.setInvalidationListener(new CachingTier.InvalidationListener<String, String>() {
      @Override
      public void onInvalidation(String key, ValueHolder<String> valueHolder) {
        expiryLatch.countDown();
        long now = System.nanoTime();
        while (!thread.getState().equals(Thread.State.BLOCKED)) {
          Thread.yield();
          if (System.nanoTime() - now > TimeUnit.MILLISECONDS.toNanos(500)) {
            assertThat(thread.getState(), is(Thread.State.BLOCKED));
          }
        }
      }
    });

    testTimeSource.advanceTime(20);

    store.iterator();
  }

  public static <V> ValueHolder<V> valueHolderValueEq(final V value) {
    return argThat(new ArgumentMatcher<ValueHolder<V>>() {

      @Override
      public boolean matches(Object argument) {
        return ((ValueHolder<V>)argument).value().equals(value);
      }
    });
  }

  private static int storeSize(OnHeapStore<?, ?> store) throws Exception {
    int counter = 0;
    Iterator<? extends Entry<?, ? extends ValueHolder<?>>> iterator = store.iterator();
    while (iterator.hasNext()) {
      iterator.next();
      counter++;
    }
    return counter;
  }

  @SuppressWarnings("unchecked")
  private static <K, V> StoreEventListener<K, V> addListener(OnHeapStore<K, V> store) {
    StoreEventListener<K, V> listener = mock(StoreEventListener.class);
    store.enableStoreEventNotifications(listener);
    return listener;
  }

  @SuppressWarnings("unchecked")
  private static <K, V> void checkExpiryEvent(StoreEventListener<K, V> listener, final K key, final V value) {
    verify(listener).onExpiration(eq(key), valueHolderValueEq(value));

  }

  private static Map<String, Long> observeAccessTimes(Iterator<Entry<String, ValueHolder<String>>> iter)
      throws CacheAccessException {
    Map<String, Long> map = new HashMap<String, Long>();
    while (iter.hasNext()) {
      Entry<String, ValueHolder<String>> entry = iter.next();
      map.put(entry.getKey(), entry.getValue().lastAccessTime(TimeUnit.MILLISECONDS));
    }
    return map;
  }

  private static Map<String, String> observe(Iterator<Entry<String, ValueHolder<String>>> iter)
      throws CacheAccessException {
    Map<String, String> map = new HashMap<String, String>();
    while (iter.hasNext()) {
      Entry<String, ValueHolder<String>> entry = iter.next();
      map.put(entry.getKey(), entry.getValue().value());
    }
    return map;
  }

  private static void assertEntry(Entry<String, ValueHolder<String>> entry, String key, String value) {
    assertThat(entry.getKey(), equalTo(key));
    assertThat(entry.getValue().value(), equalTo(value));
  }

  private void updateStoreCapacity(OnHeapStore<?, ?> store, int newCapacity) {
    CacheConfigurationChangeListener listener = store.getConfigurationChangeListeners().get(0);
    listener.cacheConfigurationChange(new CacheConfigurationChangeEvent(CacheConfigurationProperty.UPDATESIZE,
        newResourcePoolsBuilder().heap(100, EntryUnit.ENTRIES).build(),
        newResourcePoolsBuilder().heap(newCapacity, EntryUnit.ENTRIES).build()));
  }

  private static class TestTimeSource implements TimeSource {

    private long time = 0;

    @Override
    public long getTimeMillis() {
      return time;
    }

    private void advanceTime(long delta) {
      this.time += delta;
    }
  }

  protected abstract <K, V> OnHeapStore<K, V> newStore();

  protected abstract <K, V> OnHeapStore<K, V> newStore(EvictionVeto<? super K, ? super V> veto);

  protected abstract <K, V> OnHeapStore<K, V> newStore(int capacity, EvictionPrioritizer<? super K, ? super V> prioritizer);

  protected abstract <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry);

  protected abstract <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry, final EvictionVeto<? super K, ? super V> veto);

}
