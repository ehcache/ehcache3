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
package org.ehcache.impl.internal.store.heap;

import org.ehcache.Cache.Entry;
import org.ehcache.ValueSupplier;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.impl.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.impl.internal.util.StatisticsTestUtils;
import org.ehcache.core.spi.store.Store.Iterator;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.core.spi.store.Store.RemoveStatus;
import org.ehcache.core.spi.store.Store.ReplaceStatus;
import org.ehcache.core.spi.store.events.StoreEventListener;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.core.statistics.CachingTierOperationOutcomes;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.mockito.InOrder;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.ehcache.impl.internal.util.Matchers.holding;
import static org.ehcache.impl.internal.util.Matchers.valueHeld;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public abstract class BaseOnHeapStoreTest {

  private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException();
  protected StoreEventDispatcher eventDispatcher;
  protected StoreEventSink eventSink;

  @Rule
  public TestRule watchman = new TestWatcher() {
    @Override
    protected void failed(Throwable e, Description description) {
      if (e.getMessage().startsWith("test timed out after")) {
        System.err.println(buildThreadDump());
      }
    }

    private String buildThreadDump() {
      StringBuilder dump = new StringBuilder();
      dump.append("***** Test timeout - printing thread dump ****");
      Map<Thread, StackTraceElement[]> stackTraces = Thread.getAllStackTraces();
      for (Map.Entry<Thread, StackTraceElement[]> e : stackTraces.entrySet()) {
        Thread thread = e.getKey();
        dump.append(String.format(
            "\"%s\" %s prio=%d tid=%d %s\njava.lang.Thread.State: %s",
            thread.getName(),
            (thread.isDaemon() ? "daemon" : ""),
            thread.getPriority(),
            thread.getId(),
            Thread.State.WAITING.equals(thread.getState()) ?
                "in Object.wait()" : thread.getState().name().toLowerCase(),
            Thread.State.WAITING.equals(thread.getState()) ?
                "WAITING (on object monitor)" : thread.getState()));
        for (StackTraceElement stackTraceElement : e.getValue()) {
          dump.append("\n        at ");
          dump.append(stackTraceElement);
        }
        dump.append("\n");
      }
      return dump.toString();
    }
  };

  @Before
  public void setUp() {
    eventDispatcher = mock(StoreEventDispatcher.class);
    eventSink = mock(StoreEventSink.class);
    when(eventDispatcher.eventSink()).thenReturn(eventSink);
  }

  @Test
  public void testEvictEmptyStoreDoesNothing() throws Exception {
    OnHeapStore<String, String> store = newStore();
    assertThat(store.evict(eventSink), is(false));
    verify(eventSink, never()).evicted(anyString(), any(ValueSupplier.class));
  }

  @Test
  public void testEvictWithNoEvictionAdvisorDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore();
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(eventSink), is(true));
    assertThat(storeSize(store), is(99));
    verify(eventSink, times(1)).evicted(anyString(), any(ValueSupplier.class));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
  }

  @Test
  public void testEvictWithFullyAdvisedAgainstEvictionDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore(new EvictionAdvisor<String, String>() {
      @Override
      public boolean adviseAgainstEviction(String key, String value) {
        return true;
      }
    });
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(eventSink), is(true));
    assertThat(storeSize(store), is(99));
    verify(eventSink, times(1)).evicted(anyString(), any(ValueSupplier.class));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
  }

  @Test
  public void testEvictWithBrokenEvictionAdvisorDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore(new EvictionAdvisor<String, String>() {
      @Override
      public boolean adviseAgainstEviction(String key, String value) {
        throw new UnsupportedOperationException("Broken advisor!");
      }
    });
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(eventSink), is(true));
    assertThat(storeSize(store), is(99));
    verify(eventSink, times(1)).evicted(anyString(), any(ValueSupplier.class));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
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
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    timeSource.advanceTime(1);

    assertThat(store.get("key"), nullValue());
    checkExpiryEvent(eventSink, "key", "value");
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.HIT, StoreOperationOutcomes.GetOutcome.MISS));
  }

  @Test
  public void testGetNoExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(2, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    timeSource.advanceTime(1);
    assertThat(store.get("key").value(), equalTo("value"));
    verify(eventSink, never()).expired(anyString(), any(ValueSupplier.class));
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
  }

  @Test
  public void testNotContainsKey() throws Exception {
    OnHeapStore<String, String> store = newStore();
    assertThat(store.containsKey("key"), is(false));
  }

  @Test
  public void testContainsKeyExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));

    store.put("key", "value");
    timeSource.advanceTime(1);

    assertThat(store.containsKey("key"), is(false));
    checkExpiryEvent(eventSink, "key", "value");
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testPut() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    verify(eventSink).created(eq("key"), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testPutOverwrite() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");

    store.put("key", "value2");
    verify(eventSink).updated(eq("key"), argThat(holding("value")), eq("value2"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
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
    verify(eventSink).created(eq("key"), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
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
    store.put("key", "value");
    timeSource.advanceTime(1);
    ValueHolder<String> prev = store.putIfAbsent("key", "value2");
    assertThat(prev, nullValue());
    assertThat(store.get("key").value(), equalTo("value2"));
    checkExpiryEvent(eventSink, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testRemove() throws StoreAccessException {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");

    store.remove("key");
    assertThat(store.get("key"), nullValue());
    verify(eventSink).removed(eq("key"), argThat(holding("value")));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
  }

  @Test
  public void testRemoveTwoArgMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");


    RemoveStatus removed = store.remove("key", "value");
    assertThat(removed, equalTo(RemoveStatus.REMOVED));
    verify(eventSink).removed(eq("key"), argThat(holding("value")));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED));
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testRemoveTwoArgNoMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    RemoveStatus removed = store.remove("key", "not value");
    assertThat(removed, equalTo(RemoveStatus.KEY_PRESENT));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS));
    assertThat(store.get("key").value(), equalTo("value"));
  }

  @Test
  public void testRemoveTwoArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
    assertThat(store.get("key").value(), equalTo("value"));
    timeSource.advanceTime(1);
    RemoveStatus removed = store.remove("key", "value");
    assertThat(removed, equalTo(RemoveStatus.KEY_MISSING));
    checkExpiryEvent(eventSink, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testReplaceTwoArgPresent() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");

    ValueHolder<String> existing = store.replace("key", "value2");
    assertThat(existing.value(), equalTo("value"));
    assertThat(store.get("key").value(), equalTo("value2"));
    verify(eventSink).updated(eq("key"), argThat(holding("value")), eq("value2"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
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

    store.put("key", "value");
    timeSource.advanceTime(1);
    ValueHolder<String> existing = store.replace("key", "value2");
    assertThat(existing, nullValue());
    assertThat(store.get("key"), nullValue());
    checkExpiryEvent(eventSink, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testReplaceThreeArgMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");

    ReplaceStatus replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(ReplaceStatus.HIT));
    assertThat(store.get("key").value(), equalTo("value2"));
    verify(eventSink).updated(eq("key"), argThat(holding("value")), eq("value2"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED));
  }

  @Test
  public void testReplaceThreeArgNoMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();

    ReplaceStatus replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(ReplaceStatus.MISS_NOT_PRESENT));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS));

    store.put("key", "value");

    replaced = store.replace("key", "not value", "value2");
    assertThat(replaced, equalTo(ReplaceStatus.MISS_PRESENT));
    StatisticsTestUtils.validateStat(store, StoreOperationOutcomes.ConditionalReplaceOutcome.MISS, 2L);
  }

  @Test
  public void testReplaceThreeArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));

    store.put("key", "value");
    timeSource.advanceTime(1);
    ReplaceStatus replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(ReplaceStatus.MISS_NOT_PRESENT));
    assertThat(store.get("key"), nullValue());
    checkExpiryEvent(eventSink, "key", "value");
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
    store.put("key1", "value1");
    store.put("key2", "value2");
    store.put("key3", "value3");
    timeSource.advanceTime(1);

    Map<String, String> observed = observe(store.iterator());
    assertThat(3, equalTo(observed.size()));
    assertThat(observed.get("key1"), equalTo("value1"));
    assertThat(observed.get("key2"), equalTo("value2"));
    assertThat(observed.get("key3"), equalTo("value3"));

    StatisticsTestUtils.validateStat(store, StoreOperationOutcomes.ExpirationOutcome.SUCCESS, 0L);
  }

  @Test
  public void testIteratorDoesNotUpdateAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, Expirations.noExpiration());

    store.put("key1", "value1");
    store.put("key2", "value2");

    timeSource.advanceTime(5);

    Map<String, Long> times = observeAccessTimes(store.iterator());
    assertThat(2, equalTo(times.size()));
    assertThat(times.get("key1"), equalTo(0L));
    assertThat(times.get("key2"), equalTo(0L));
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
    verify(eventSink).updated(eq("key"), argThat(holding("value")), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
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
    verify(eventSink).created(eq("key"), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
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
    verify(eventSink).removed(eq("key"), argThat(holding("value")));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
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
    } catch (StoreAccessException cae) {
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
    verify(eventSink).updated(eq("key"), argThat(holding("value")), eq("value2"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.PUT));
    assertThat(store.get("key").value(), equalTo("value2"));
  }

  @Test
  public void testComputeExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    store.put("key", "value");
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
    checkExpiryEvent(eventSink, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testComputeWhenExpireOnCreate() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(1000L);
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.ZERO;
      }

      @Override
      public Duration getExpiryForAccess(String key, ValueSupplier<? extends String> value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForUpdate(String key, ValueSupplier<? extends String> oldValue, String newValue) {
        return Duration.INFINITE;
      }
    });

    ValueHolder<String> result = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String key, String value) {
        return "value";
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return false;
      }
    });
    assertThat(result, nullValue());
  }

  @Test
  public void testComputeWhenExpireOnUpdate() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(1000L);
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(String key, ValueSupplier<? extends String> value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForUpdate(String key, ValueSupplier<? extends String> oldValue, String newValue) {
        return Duration.ZERO;
      }
    });

    store.put("key", "value");
    ValueHolder<String> result = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String key, String value) {
        return "newValue";
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return false;
      }
    });
    assertThat(result, valueHeld("newValue"));
  }

  @Test
  public void testComputeWhenExpireOnAccess() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(1000L);
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(String key, ValueSupplier<? extends String> value) {
        return Duration.ZERO;
      }

      @Override
      public Duration getExpiryForUpdate(String key, ValueSupplier<? extends String> oldValue, String newValue) {
        return Duration.INFINITE;
      }
    });

    store.put("key", "value");
    ValueHolder<String> result = store.compute("key", new BiFunction<String, String, String>() {
      @Override
      public String apply(String key, String value) {
        return value;
      }
    }, new NullaryFunction<Boolean>() {
      @Override
      public Boolean apply() {
        return false;
      }
    });
    assertThat(result, valueHeld("value"));
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
    verify(eventSink).created(eq("key"), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
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
    } catch (StoreAccessException cae) {
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
    verify(eventSink).expired(eq("key"), argThat(holding(value)));
    verify(eventSink).created(eq("key"), eq("value2"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
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
      public Duration getExpiryForAccess(String key, ValueSupplier<? extends String> value) {
        throw new AssertionError();
      }

      @Override
      public Duration getExpiryForUpdate(String key, ValueSupplier<? extends String> oldValue, String newValue) {
        throw new AssertionError();
      }
    });

    store.put("key", "value");
    assertThat(store.containsKey("key"), equalTo(false));
  }

  @Test
  public void testExpiryAccessExceptionReturnsValueAndExpiresIt() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(5);
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, String>() {

      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(String key, ValueSupplier<? extends String> value) {
        throw RUNTIME_EXCEPTION;
      }

      @Override
      public Duration getExpiryForUpdate(String key, ValueSupplier<? extends String> oldValue, String newValue) {
        return null;
      }
    });

    store.put("key", "value");
    assertThat(store.get("key"), valueHeld("value"));
    assertNull(store.get("key"));
  }

  @Test
  public void testExpiryUpdateException() throws Exception{
    final TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, new Expiry<String, String>() {
      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(String key, ValueSupplier<? extends String> value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForUpdate(String key, ValueSupplier<? extends String> oldValue, String newValue) {
        if (timeSource.getTimeMillis() > 0) {
          throw new RuntimeException();
        }
        return Duration.INFINITE;
      }
    });

    store.put("key", "value");
    store.get("key");
    timeSource.advanceTime(1000);

    store.put("key", "newValue");
    assertNull(store.get("key"));
  }

  @Test
  public void testGetOrComputeIfAbsentExpiresOnHit() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
        Expirations.timeToLiveExpiration(new Duration(1, TimeUnit.MILLISECONDS)));
    CachingTier.InvalidationListener invalidationListener = mock(CachingTier.InvalidationListener.class);
    store.setInvalidationListener(invalidationListener);

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
    verify(invalidationListener).onInvalidation(eq("key"), argThat(valueHeld("value")));
    assertThat(storeSize(store), is(0));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testGetOrComputeIfAbsentRemovesFault() throws StoreAccessException {
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
          store.invalidate("1");
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
        } catch (StoreAccessException e) {
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
  public void testGetOrComputeIfAbsentInvalidatesFault() throws StoreAccessException, InterruptedException {
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
        } catch (StoreAccessException e) {
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
              return new CopiedOnHeapValueHolder<String>("theAnswer!", System.currentTimeMillis(), -1, false, new IdentityCopier<String>());
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
              return new CopiedOnHeapValueHolder<String>("TheAnswer!", System.currentTimeMillis(), false, new IdentityCopier<String>());
            }
          });
        } catch (StoreAccessException caex) {
          failedInThread.set(new AssertionError("StoreAccessException: " + caex.getMessage()));
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
              return new CopiedOnHeapValueHolder<String>("TheAnswer!", System.currentTimeMillis(), false, new IdentityCopier<String>());
            }
          });
        } catch (StoreAccessException caex) {
          failedInThread.set(new AssertionError("StoreAccessException: " + caex.getMessage()));
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

  @Test(timeout = 2000L)
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
        } catch (StoreAccessException e) {
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
            }
            assertThat(thread.getState(), is(Thread.State.BLOCKED));
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      }
    });

    //trigger eviction
    store.put("keyC", "valueC");
  }

  @Test(timeout = 2000L)
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
        } catch (StoreAccessException e) {
          e.printStackTrace();
        }
      }
    });
    thread.start();

    store.setInvalidationListener(new CachingTier.InvalidationListener<String, String>() {
      @Override
      public void onInvalidation(String key, ValueHolder<String> valueHolder) {
        expiryLatch.countDown();
        while (!thread.getState().equals(Thread.State.BLOCKED)) {
          Thread.yield();
        }
        assertThat(thread.getState(), is(Thread.State.BLOCKED));
      }
    });

    testTimeSource.advanceTime(20);

    store.iterator();
  }

  private void verifyListenerReleaseEventsInOrder(StoreEventDispatcher<String, String> listener) {
    InOrder inOrder = inOrder(listener);
    inOrder.verify(listener).eventSink();
    inOrder.verify(listener).releaseEventSink(eventSink);
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
  private <K, V> StoreEventListener<K, V> addListener(OnHeapStore<K, V> store) {
    eventDispatcher = mock(StoreEventDispatcher.class);
    eventSink = mock(StoreEventSink.class);
    when(eventDispatcher.eventSink()).thenReturn(eventSink);
    StoreEventListener<K, V> listener = mock(StoreEventListener.class);
    return listener;
  }

  @SuppressWarnings("unchecked")
  private static <K, V> void checkExpiryEvent(StoreEventSink<K, V> listener, final K key, final V value) {
    verify(listener).expired(eq(key), argThat(holding(value)));

  }

  private static Map<String, Long> observeAccessTimes(Iterator<Entry<String, ValueHolder<String>>> iter)
      throws StoreAccessException {
    Map<String, Long> map = new HashMap<String, Long>();
    while (iter.hasNext()) {
      Entry<String, ValueHolder<String>> entry = iter.next();
      map.put(entry.getKey(), entry.getValue().lastAccessTime(TimeUnit.MILLISECONDS));
    }
    return map;
  }

  private static Map<String, String> observe(Iterator<Entry<String, ValueHolder<String>>> iter)
      throws StoreAccessException {
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

  protected <K, V> OnHeapStore<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), Eviction.noAdvice());
  }

  protected <K, V> OnHeapStore<K, V> newStore(EvictionAdvisor<? super K, ? super V> evictionAdvisor) {
    return newStore(SystemTimeSource.INSTANCE, Expirations.noExpiration(), evictionAdvisor);
  }

  protected <K, V> OnHeapStore<K, V> newStore(TimeSource timeSource, Expiry<? super K, ? super V> expiry) {
    return newStore(timeSource, expiry, Eviction.noAdvice());
  }

  protected abstract void updateStoreCapacity(OnHeapStore<?, ?> store, int newCapacity);

  protected abstract <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final Expiry<? super K, ? super V> expiry, final EvictionAdvisor<? super K, ? super V> evictionAdvisor);
}
