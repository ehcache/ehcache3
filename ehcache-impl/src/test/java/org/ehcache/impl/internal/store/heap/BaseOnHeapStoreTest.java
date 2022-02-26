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
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.core.events.StoreEventDispatcher;
import org.ehcache.core.events.StoreEventSink;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.expiry.ExpiryPolicy;
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

import java.time.Duration;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Exchanger;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.ehcache.config.builders.ExpiryPolicyBuilder.expiry;
import static org.ehcache.impl.internal.util.Matchers.holding;
import static org.ehcache.impl.internal.util.Matchers.valueHeld;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

public abstract class BaseOnHeapStoreTest {

  private static final RuntimeException RUNTIME_EXCEPTION = new RuntimeException();
  protected StoreEventDispatcher<Object, Object> eventDispatcher;
  protected StoreEventSink<Object, Object> eventSink;

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
  @SuppressWarnings("unchecked")
  public void setUp() {
    eventDispatcher = mock(StoreEventDispatcher.class);
    eventSink = mock(StoreEventSink.class);
    when(eventDispatcher.eventSink()).thenReturn(eventSink);
  }

  @Test
  public void testEvictEmptyStoreDoesNothing() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    assertThat(store.evict(eventSink), is(false));
    verify(eventSink, never()).evicted(anyString(), anyValueSupplier());
  }

  @Test
  public void testEvictWithNoEvictionAdvisorDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(eventSink), is(true));
    assertThat(storeSize(store), is(99));
    verify(eventSink, times(1)).evicted(anyString(), anyValueSupplier());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
  }

  @Test
  public void testEvictWithFullyAdvisedAgainstEvictionDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore((key, value) -> true);
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(eventSink), is(true));
    assertThat(storeSize(store), is(99));
    verify(eventSink, times(1)).evicted(anyString(), anyValueSupplier());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
  }

  @Test
  public void testEvictWithBrokenEvictionAdvisorDoesEvict() throws Exception {
    OnHeapStore<String, String> store = newStore((key, value) -> {
      throw new UnsupportedOperationException("Broken advisor!");
    });
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    for (int i = 0; i < 100; i++) {
      store.put(Integer.toString(i), Integer.toString(i));
    }
    assertThat(store.evict(eventSink), is(true));
    assertThat(storeSize(store), is(99));
    verify(eventSink, times(1)).evicted(anyString(), anyValueSupplier());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.EvictionOutcome.SUCCESS));
  }

  @Test
  public void testGet() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    assertThat(store.get("key").get(), equalTo("value"));
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
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();
    OnHeapStore<String, String> store = newStore(timeSource,
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));
    store.put("key", "value");
    assertThat(store.get("key").get(), equalTo("value"));
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
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(2)));
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    store.put("key", "value");
    timeSource.advanceTime(1);
    assertThat(store.get("key").get(), equalTo("value"));
    verify(eventSink, never()).expired(anyString(), anyValueSupplier());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.GetOutcome.HIT));
  }

  @Test
  public void testAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, ExpiryPolicyBuilder.noExpiration());
    store.put("key", "value");

    long first = store.get("key").lastAccessTime();
    assertThat(first, equalTo(timeSource.getTimeMillis()));
    final long advance = 5;
    timeSource.advanceTime(advance);
    long next = store.get("key").lastAccessTime();
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
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();
    OnHeapStore<String, String> store = newStore(timeSource,
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));

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
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    store.put("key", "value");
    verify(eventSink).created(eq("key"), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT));
    assertThat(store.get("key").get(), equalTo("value"));
  }

  @Test
  public void testPutOverwrite() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    store.put("key", "value");

    store.put("key", "value2");
    verify(eventSink).updated(eq("key"), argThat(holding("value")), eq("value2"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    assertThat(store.get("key").get(), equalTo("value2"));
  }

  @Test
  public void testCreateTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, ExpiryPolicyBuilder.noExpiration());
    assertThat(store.containsKey("key"), is(false));
    store.put("key", "value");
    ValueHolder<String> valueHolder = store.get("key");
    assertThat(timeSource.getTimeMillis(), equalTo(valueHolder.creationTime()));
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
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    ValueHolder<String> prev = store.putIfAbsent("key", "value", b -> {});

    assertThat(prev, nullValue());
    verify(eventSink).created(eq("key"), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT));
    assertThat(store.get("key").get(), equalTo("value"));
  }

  @Test
  public void testPutIfAbsentValuePresent() throws Exception {
    OnHeapStore<String, String> store = newStore();
    store.put("key", "value");
    ValueHolder<String> prev = store.putIfAbsent("key", "value2", b -> {});
    assertThat(prev.get(), equalTo("value"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.HIT));
  }

  @Test
  public void testPutIfAbsentUpdatesAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, ExpiryPolicyBuilder.noExpiration());
    assertThat(store.get("key"), nullValue());
    store.putIfAbsent("key", "value", b -> {});
    long first = store.get("key").lastAccessTime();
    timeSource.advanceTime(1);
    long next = store.putIfAbsent("key", "value2", b -> {}).lastAccessTime();
    assertThat(next - first, equalTo(1L));
  }

  @Test
  public void testPutIfAbsentExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));
    store.put("key", "value");
    timeSource.advanceTime(1);
    ValueHolder<String> prev = store.putIfAbsent("key", "value2", b -> {});
    assertThat(prev, nullValue());
    assertThat(store.get("key").get(), equalTo("value2"));
    checkExpiryEvent(getStoreEventSink(), "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testRemove() throws StoreAccessException {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();
    store.put("key", "value");

    store.remove("key");
    assertThat(store.get("key"), nullValue());
    verify(eventSink).removed(eq("key"), argThat(holding("value")));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
  }

  @Test
  public void testRemoveTwoArgMatch() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();
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
    assertThat(store.get("key").get(), equalTo("value"));
  }

  @Test
  public void testRemoveTwoArgExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    OnHeapStore<String, String> store = newStore(timeSource,
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));

    store.put("key", "value");
    assertThat(store.get("key").get(), equalTo("value"));
    timeSource.advanceTime(1);
    RemoveStatus removed = store.remove("key", "value");
    assertThat(removed, equalTo(RemoveStatus.KEY_MISSING));
    checkExpiryEvent(eventSink, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testReplaceTwoArgPresent() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    store.put("key", "value");

    ValueHolder<String> existing = store.replace("key", "value2");
    assertThat(existing.get(), equalTo("value"));
    assertThat(store.get("key").get(), equalTo("value2"));
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
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    OnHeapStore<String, String> store = newStore(timeSource,
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));

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
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    store.put("key", "value");

    ReplaceStatus replaced = store.replace("key", "value", "value2");
    assertThat(replaced, equalTo(ReplaceStatus.HIT));
    assertThat(store.get("key").get(), equalTo("value2"));
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
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    OnHeapStore<String, String> store = newStore(timeSource,
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));

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
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));
    store.put("key1", "value1");
    store.put("key2", "value2");
    store.put("key3", "value3");
    timeSource.advanceTime(1);

    Map<String, String> observed = observe(store.iterator());
    assertThat(0, equalTo(observed.size()));

    StatisticsTestUtils.validateStat(store, StoreOperationOutcomes.ExpirationOutcome.SUCCESS, 3L);
  }

  @Test
  public void testIteratorDoesNotUpdateAccessTime() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, ExpiryPolicyBuilder.noExpiration());

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
    OnHeapStore<String, String> store = newStore(timeSource, ExpiryPolicyBuilder.noExpiration());
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    store.put("key", "value");
    ValueHolder<String> installedHolder = store.get("key");
    long createTime = installedHolder.creationTime();
    long accessTime = installedHolder.lastAccessTime();
    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.computeAndGet("key", (mappedKey, mappedValue) -> mappedValue, () -> true, () -> false);

    assertThat(newValue.get(), equalTo("value"));
    assertThat(createTime + 1, equalTo(newValue.creationTime()));
    assertThat(accessTime + 1, equalTo(newValue.lastAccessTime()));
    verify(eventSink).updated(eq("key"), argThat(holding("value")), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.PUT));
  }

  @Test
  public void testComputeReplaceFalse() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, ExpiryPolicyBuilder.noExpiration());

    store.put("key", "value");
    ValueHolder<String> installedHolder = store.get("key");
    long createTime = installedHolder.creationTime();
    long accessTime = installedHolder.lastAccessTime();
    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.computeAndGet("key", (mappedKey, mappedValue) -> mappedValue, () -> false, () -> false);

    assertThat(newValue.get(), equalTo("value"));
    assertThat(createTime, equalTo(newValue.creationTime()));
    assertThat(accessTime + 1, equalTo(newValue.lastAccessTime()));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.HIT));
  }

  @Test
  public void testGetAndCompute() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    ValueHolder<String> oldValue = store.getAndCompute("key", (mappedKey, mappedValue) -> {
      assertThat(mappedKey, equalTo("key"));
      assertThat(mappedValue, nullValue());
      return "value";
    });

    assertThat(oldValue, nullValue());
    verify(eventSink).created(eq("key"), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.PUT));
    assertThat(store.get("key").get(), equalTo("value"));
  }

  @Test
  public void testGetAndComputeNull() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    ValueHolder<String> oldValue = store.getAndCompute("key", (mappedKey, mappedValue) -> null);

    assertThat(oldValue, nullValue());
    assertThat(store.get("key"), nullValue());
    StatisticsTestUtils.validateStat(store, StoreOperationOutcomes.ComputeOutcome.MISS, 1L);

    store.put("key", "value");

    oldValue = store.getAndCompute("key", (mappedKey, mappedValue) -> null);

    assertThat(oldValue.get(), equalTo("value"));
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
      store.getAndCompute("key", (mappedKey, mappedValue) -> {
        throw RUNTIME_EXCEPTION;
      });
      fail("RuntimeException expected");
    } catch (StoreAccessException cae) {
      assertThat(cae.getCause(), is((Throwable)RUNTIME_EXCEPTION));
    }
    assertThat(store.get("key").get(), equalTo("value"));
  }

  @Test
  public void testGetAndComputeExistingValue() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    store.put("key", "value");

    ValueHolder<String> oldValue = store.getAndCompute("key", (mappedKey, mappedValue) -> {
      assertThat(mappedKey, equalTo("key"));
      assertThat(mappedValue, equalTo("value"));
      return "value2";
    });

    assertThat(oldValue.get(), equalTo("value"));
    verify(eventSink).updated(eq("key"), argThat(holding("value")), eq("value2"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeOutcome.PUT));
    assertThat(store.get("key").get(), equalTo("value2"));
  }

  @Test
  public void testGetAndComputeExpired() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    OnHeapStore<String, String> store = newStore(timeSource,
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));
    store.put("key", "value");
    timeSource.advanceTime(1);
    ValueHolder<String> oldValue = store.getAndCompute("key", (mappedKey, mappedValue) -> {
      assertThat(mappedKey, equalTo("key"));
      assertThat(mappedValue, nullValue());
      return "value2";
    });

    assertThat(oldValue, nullValue());
    assertThat(store.get("key").get(), equalTo("value2"));
    checkExpiryEvent(eventSink, "key", "value");
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testComputeWhenExpireOnCreate() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(1000L);
    OnHeapStore<String, String> store = newStore(timeSource, expiry().create(Duration.ZERO).build());

    ValueHolder<String> result = store.computeAndGet("key", (key, value) -> "value", () -> false, () -> false);
    assertThat(result, nullValue());
  }

  @Test
  public void testComputeWhenExpireOnUpdate() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(1000L);
    OnHeapStore<String, String> store = newStore(timeSource, expiry().update(Duration.ZERO).build());

    store.put("key", "value");
    ValueHolder<String> result = store.computeAndGet("key", (key, value) -> "newValue", () -> false, () -> false);
    assertThat(result, valueHeld("newValue"));
  }

  @Test
  public void testComputeWhenExpireOnAccess() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(1000L);
    OnHeapStore<String, String> store = newStore(timeSource, expiry().access(Duration.ZERO).build());

    store.put("key", "value");
    ValueHolder<String> result = store.computeAndGet("key", (key, value) -> value, () -> false, () -> false);
    assertThat(result, valueHeld("value"));
  }

  @Test
  public void testComputeIfAbsent() throws Exception {
    OnHeapStore<String, String> store = newStore();
    StoreEventSink<String, String> eventSink = getStoreEventSink();
    StoreEventDispatcher<String, String> eventDispatcher = getStoreEventDispatcher();

    ValueHolder<String> newValue = store.computeIfAbsent("key", mappedKey -> {
      assertThat(mappedKey, equalTo("key"));
      return "value";
    });

    assertThat(newValue.get(), equalTo("value"));
    verify(eventSink).created(eq("key"), eq("value"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.PUT));
    assertThat(store.get("key").get(), equalTo("value"));
  }

  @Test
  public void testComputeIfAbsentExisting() throws Exception {
    OnHeapStore<String, String> store = newStore();

    store.put("key", "value");
    ValueHolder<String> newValue = store.computeIfAbsent("key", mappedKey -> {
      fail("Should not be called");
      return null;
    });

    assertThat(newValue.get(), equalTo("value"));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.HIT));
    assertThat(store.get("key").get(), equalTo("value"));
  }

  @Test
  public void testComputeIfAbsentReturnNull() throws Exception {
    OnHeapStore<String, String> store = newStore();

    ValueHolder<String> newValue = store.computeIfAbsent("key", mappedKey -> null);

    assertThat(newValue, nullValue());
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.NOOP));
    assertThat(store.get("key"), nullValue());
  }

  @Test
  public void testComputeIfAbsentException() throws Exception {
    OnHeapStore<String, String> store = newStore();

    try {
      store.computeIfAbsent("key", mappedKey -> {
        throw RUNTIME_EXCEPTION;
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
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));
    store.put("key", "value");

    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.computeIfAbsent("key", mappedKey -> {
      assertThat(mappedKey, equalTo("key"));
      return "value2";
    });

    assertThat(newValue.get(), equalTo("value2"));
    assertThat(store.get("key").get(), equalTo("value2"));
    final String value = "value";
    verify(eventSink).expired(eq("key"), argThat(holding(value)));
    verify(eventSink).created(eq("key"), eq("value2"));
    verifyListenerReleaseEventsInOrder(eventDispatcher);
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testExpiryCreateException() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource, new ExpiryPolicy<String, String>() {

      @Override
      public Duration getExpiryForCreation(String key, String value) {
        throw RUNTIME_EXCEPTION;
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

    store.put("key", "value");
    assertThat(store.containsKey("key"), equalTo(false));
  }

  @Test
  public void testExpiryAccessExceptionReturnsValueAndExpiresIt() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    timeSource.advanceTime(5);
    OnHeapStore<String, String> store = newStore(timeSource, new ExpiryPolicy<String, String>() {

      @Override
      public Duration getExpiryForCreation(String key, String value) {
        return ExpiryPolicy.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(String key, Supplier<? extends String> value) {
        throw RUNTIME_EXCEPTION;
      }

      @Override
      public Duration getExpiryForUpdate(String key, Supplier<? extends String> oldValue, String newValue) {
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
    OnHeapStore<String, String> store = newStore(timeSource, new ExpiryPolicy<String, String>() {
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
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));
    @SuppressWarnings("unchecked")
    CachingTier.InvalidationListener<String, String> invalidationListener = mock(CachingTier.InvalidationListener.class);
    store.setInvalidationListener(invalidationListener);

    store.put("key", "value");
    assertThat(storeSize(store), is(1));
    timeSource.advanceTime(1);

    ValueHolder<String> newValue = store.getOrComputeIfAbsent("key", s -> null);

    assertThat(newValue, nullValue());
    assertThat(store.get("key"), nullValue());
    verify(invalidationListener).onInvalidation(eq("key"), argThat(valueHeld("value")));
    assertThat(storeSize(store), is(0));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testGetOfComputeIfAbsentExpiresWithLoaderWriter() throws Exception {
    TestTimeSource timeSource = new TestTimeSource();
    OnHeapStore<String, String> store = newStore(timeSource,
      ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(1)));
    @SuppressWarnings("unchecked")
    CachingTier.InvalidationListener<String, String> invalidationListener = mock(CachingTier.InvalidationListener.class);
    store.setInvalidationListener(invalidationListener);

    // Add an entry
    store.put("key", "value");
    assertThat(storeSize(store), is(1));

    // Advance after expiration time
    timeSource.advanceTime(1);

    @SuppressWarnings("unchecked")
    final ValueHolder<String> vh = mock(ValueHolder.class);
    when(vh.get()).thenReturn("newvalue");
    when(vh.expirationTime()).thenReturn(2L);

    ValueHolder<String> newValue = store.getOrComputeIfAbsent("key", s -> vh);

    assertThat(newValue, valueHeld("newvalue"));
    assertThat(store.get("key"), valueHeld("newvalue"));
    verify(invalidationListener).onInvalidation(eq("key"), argThat(valueHeld("value")));
    assertThat(storeSize(store), is(1));
    StatisticsTestUtils.validateStats(store, EnumSet.of(StoreOperationOutcomes.ExpirationOutcome.SUCCESS));
  }

  @Test
  public void testGetOrComputeIfAbsentRemovesFault() throws StoreAccessException {
    final OnHeapStore<String, String> store = newStore();
    final CountDownLatch testCompletionLatch = new CountDownLatch(1);
    final CountDownLatch threadFaultCompletionLatch = new CountDownLatch(1);
    final CountDownLatch mainFaultCreationLatch = new CountDownLatch(1);


    Thread thread = new Thread(() -> {
      try {
        try {
          mainFaultCreationLatch.await();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        store.invalidate("1");
        store.getOrComputeIfAbsent("1", s -> {
          threadFaultCompletionLatch.countDown();
          try {
            testCompletionLatch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          return null;
        });
      } catch (StoreAccessException e) {
        e.printStackTrace();
      }
    });
    thread.start();

    store.getOrComputeIfAbsent("1", s -> {
      try {
        mainFaultCreationLatch.countDown();
        threadFaultCompletionLatch.await();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      return null;
    });
    testCompletionLatch.countDown();
  }

  @Test
  public void testGetOrComputeIfAbsentInvalidatesFault() throws StoreAccessException, InterruptedException {
    final OnHeapStore<String, String> store = newStore();
    final CountDownLatch testCompletionLatch = new CountDownLatch(1);
    final CountDownLatch threadFaultCompletionLatch = new CountDownLatch(1);
    CachingTier.InvalidationListener<String, String> invalidationListener = (key, valueHolder) -> {
      try {
        valueHolder.getId();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Test tried to invalidate Fault");
      }
    };

    store.setInvalidationListener(invalidationListener);
    updateStoreCapacity(store, 1);

    Thread thread = new Thread(() -> {
      try {
        store.getOrComputeIfAbsent("1", s -> {
          threadFaultCompletionLatch.countDown();
          try {
            testCompletionLatch.await();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
          return null;
        });
      } catch (StoreAccessException e) {
        e.printStackTrace();
      }
    });
    thread.start();

    threadFaultCompletionLatch.await();
    store.getOrComputeIfAbsent("10", s -> null);
    testCompletionLatch.countDown();
  }

  @Test
  public void testGetOrComputeIfAbsentContention() throws InterruptedException {

    final OnHeapStore<String, String> store = newStore();

    int threads = 10;
    final CountDownLatch startLatch = new CountDownLatch(1);
    final CountDownLatch endLatch = new CountDownLatch(threads);

    Runnable runnable = () -> {
      try {
        startLatch.await();
      } catch (InterruptedException e) {
        fail("Got an exception waiting to start thread " + e);
      }
      try {
        ValueHolder<String> result = store.getOrComputeIfAbsent("42", key -> new CopiedOnHeapValueHolder<>("theAnswer!", System
          .currentTimeMillis(), -1, false, new IdentityCopier<>()));
        assertThat(result.get(), is("theAnswer!"));
        endLatch.countDown();
      } catch (Exception e) {
        e.printStackTrace();
        fail("Got an exception " + e);
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
  @SuppressWarnings("unchecked")
  public void testConcurrentFaultingAndInvalidate() throws Exception {
    final OnHeapStore<String, String> store = newStore();
    CachingTier.InvalidationListener<String, String> invalidationListener = mock(CachingTier.InvalidationListener.class);
    store.setInvalidationListener(invalidationListener);

    final AtomicReference<AssertionError> failedInThread = new AtomicReference<>();

    final CountDownLatch getLatch = new CountDownLatch(1);
    final CountDownLatch invalidateLatch = new CountDownLatch(1);

    new Thread(() -> {
      try {
        store.getOrComputeIfAbsent("42", aString -> {
          invalidateLatch.countDown();
          try {
            boolean await = getLatch.await(5, TimeUnit.SECONDS);
            if (!await) {
              failedInThread.set(new AssertionError("latch timed out"));
            }
          } catch (InterruptedException e) {
            failedInThread.set(new AssertionError("Interrupted exception: " + e.getMessage()));
          }
          return new CopiedOnHeapValueHolder<>("TheAnswer!", System.currentTimeMillis(), false, new IdentityCopier<>());
        });
      } catch (StoreAccessException caex) {
        failedInThread.set(new AssertionError("StoreAccessException: " + caex.getMessage()));
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

    final AtomicReference<AssertionError> failedInThread = new AtomicReference<>();

    final CountDownLatch getLatch = new CountDownLatch(1);
    final CountDownLatch invalidateLatch = new CountDownLatch(1);

    new Thread(() -> {
      try {
        store.getOrComputeIfAbsent("42", aString -> {
          invalidateLatch.countDown();
          try {
            boolean await = getLatch.await(5, TimeUnit.SECONDS);
            if (!await) {
              failedInThread.set(new AssertionError("latch timed out"));
            }
          } catch (InterruptedException e) {
            failedInThread.set(new AssertionError("Interrupted exception: " + e.getMessage()));
          }
          return new CopiedOnHeapValueHolder<>("TheAnswer!", System.currentTimeMillis(), false, new IdentityCopier<>());
        });
      } catch (StoreAccessException caex) {
        failedInThread.set(new AssertionError("StoreAccessException: " + caex.getMessage()));
      }
    }).start();

    boolean await = invalidateLatch.await(5, TimeUnit.SECONDS);
    if (!await) {
      fail("latch timed out");
    }
    store.silentInvalidate("42", stringValueHolder -> {
      if (stringValueHolder != null) {
        assertThat("Expected a null parameter otherwise we leak a Fault", stringValueHolder, nullValue());
      }
      return null;
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

    final Exchanger<String> keyExchanger = new Exchanger<>();
    final AtomicReference<String> reference = new AtomicReference<>();
    final CountDownLatch faultingLatch = new CountDownLatch(1);

    // prepare concurrent faulting
    final Thread thread = new Thread(() -> {
      String key;

      try {
        key = keyExchanger.exchange("ready to roll!");
        store.put(key, "updateValue");
      } catch (InterruptedException | StoreAccessException e) {
        e.printStackTrace();
      }
    });
    thread.start();

    store.setInvalidationListener((key, valueHolder) -> {
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
    });

    //trigger eviction
    store.put("keyC", "valueC");
  }

  @Test(timeout = 2000L)
  public void testIteratorExpiryHappensUnderExpiredKeyLockScope() throws Exception {
    TestTimeSource testTimeSource = new TestTimeSource();
    final OnHeapStore<String, String> store = newStore(testTimeSource, ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofMillis(10)));

    store.put("key", "value");

    final CountDownLatch expiryLatch = new CountDownLatch(1);

    final Thread thread = new Thread(() -> {
      try {
        expiryLatch.await();
        store.put("key", "newValue");
      } catch (InterruptedException | StoreAccessException e) {
        e.printStackTrace();
      }
    });
    thread.start();

    store.setInvalidationListener((key, valueHolder) -> {
      expiryLatch.countDown();
      while (!thread.getState().equals(Thread.State.BLOCKED)) {
        Thread.yield();
      }
      assertThat(thread.getState(), is(Thread.State.BLOCKED));
    });

    testTimeSource.advanceTime(20);

    store.iterator();
  }

  @SuppressWarnings("unchecked")
  private Supplier<String> anyValueSupplier() {
    return any(Supplier.class);
  }

  private <K, V> void verifyListenerReleaseEventsInOrder(StoreEventDispatcher<K, V> listener) {
    StoreEventSink<K, V> eventSink = getStoreEventSink();

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
    @SuppressWarnings("unchecked")
    StoreEventListener<K, V> listener = mock(StoreEventListener.class);
    return listener;
  }

  @SuppressWarnings("unchecked")
  private static <K, V> void checkExpiryEvent(StoreEventSink<K, V> listener, final K key, final V value) {
    verify(listener).expired(eq(key), argThat(holding(value)));

  }

  private static Map<String, Long> observeAccessTimes(Iterator<Entry<String, ValueHolder<String>>> iter)
      throws StoreAccessException {
    Map<String, Long> map = new HashMap<>();
    while (iter.hasNext()) {
      Entry<String, ValueHolder<String>> entry = iter.next();
      map.put(entry.getKey(), entry.getValue().lastAccessTime());
    }
    return map;
  }

  private static Map<String, String> observe(Iterator<Entry<String, ValueHolder<String>>> iter)
      throws StoreAccessException {
    Map<String, String> map = new HashMap<>();
    while (iter.hasNext()) {
      Entry<String, ValueHolder<String>> entry = iter.next();
      map.put(entry.getKey(), entry.getValue().get());
    }
    return map;
  }

  private static void assertEntry(Entry<String, ValueHolder<String>> entry, String key, String value) {
    assertThat(entry.getKey(), equalTo(key));
    assertThat(entry.getValue().get(), equalTo(value));
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

  @SuppressWarnings("unchecked")
  protected <K, V> StoreEventSink<K, V> getStoreEventSink() {
    return (StoreEventSink<K, V>) eventSink;
  }

  @SuppressWarnings("unchecked")
  protected <K, V> StoreEventDispatcher<K, V> getStoreEventDispatcher() {
    return (StoreEventDispatcher<K, V>) eventDispatcher;
  }

  protected <K, V> OnHeapStore<K, V> newStore() {
    return newStore(SystemTimeSource.INSTANCE, ExpiryPolicyBuilder.noExpiration(), Eviction.noAdvice());
  }

  protected <K, V> OnHeapStore<K, V> newStore(EvictionAdvisor<? super K, ? super V> evictionAdvisor) {
    return newStore(SystemTimeSource.INSTANCE, ExpiryPolicyBuilder.noExpiration(), evictionAdvisor);
  }

  protected <K, V> OnHeapStore<K, V> newStore(TimeSource timeSource, ExpiryPolicy<? super K, ? super V> expiry) {
    return newStore(timeSource, expiry, Eviction.noAdvice());
  }

  protected abstract void updateStoreCapacity(OnHeapStore<?, ?> store, int newCapacity);

  protected abstract <K, V> OnHeapStore<K, V> newStore(final TimeSource timeSource,
      final ExpiryPolicy<? super K, ? super V> expiry, final EvictionAdvisor<? super K, ? super V> evictionAdvisor);
}
