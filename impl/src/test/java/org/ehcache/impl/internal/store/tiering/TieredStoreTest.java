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
package org.ehcache.impl.internal.store.tiering;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.RemoveStatus;
import org.ehcache.core.spi.store.Store.ReplaceStatus;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.tiering.AuthoritativeTier;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link TieredStore}.
 */
public class TieredStoreTest {

  @Mock
  private CachingTier<Number, CharSequence> numberCachingTier;
  @Mock
  private AuthoritativeTier<Number, CharSequence> numberAuthoritativeTier;
  @Mock
  private CachingTier<String, String> stringCachingTier;
  @Mock
  private AuthoritativeTier<String, String> stringAuthoritativeTier;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetHitsCachingTier() throws Exception {
    when(numberCachingTier.getOrComputeIfAbsent(eq(1), any(Function.class))).thenReturn(newValueHolder("one"));

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.get(1).get(), Matchers.equalTo("one"));

    verify(numberAuthoritativeTier, times(0)).getAndFault(any(Number.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetHitsAuthoritativeTier() throws Exception {
    Store.ValueHolder<CharSequence> valueHolder = newValueHolder("one");
    when(numberAuthoritativeTier.getAndFault(eq(1))).thenReturn(valueHolder);
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).then((Answer<Store.ValueHolder<CharSequence>>) invocation -> {
      Number key = (Number) invocation.getArguments()[0];
      Function<Number, Store.ValueHolder<CharSequence>> function = (Function<Number, Store.ValueHolder<CharSequence>>) invocation.getArguments()[1];
      return function.apply(key);
    });

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.get(1).get(), Matchers.equalTo("one"));

    verify(numberCachingTier, times(1)).getOrComputeIfAbsent(eq(1), any(Function.class));
    verify(numberAuthoritativeTier, times(1)).getAndFault(any(Number.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetMisses() throws Exception {
    when(numberAuthoritativeTier.getAndFault(eq(1))).thenReturn(null);
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).then((Answer<Store.ValueHolder<CharSequence>>) invocation -> {
      Number key = (Number) invocation.getArguments()[0];
      Function<Number, Store.ValueHolder<CharSequence>> function = (Function<Number, Store.ValueHolder<CharSequence>>) invocation.getArguments()[1];
      return function.apply(key);
    });

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.get(1), is(nullValue()));

    verify(numberCachingTier, times(1)).getOrComputeIfAbsent(eq(1), any(Function.class));
    verify(numberAuthoritativeTier, times(1)).getAndFault(any(Number.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetThrowsRuntimeException() throws Exception {
    RuntimeException error = new RuntimeException();
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenThrow(new StoreAccessException(error));

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    try {
      tieredStore.get(1);
      fail("We should get an Error");
    } catch (RuntimeException e) {
      assertSame(error, e);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetThrowsError() throws Exception {
    Error error = new Error();
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenThrow(new StoreAccessException(error));

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    try {
      tieredStore.get(1);
      fail("We should get an Error");
    } catch (Error e) {
      assertSame(error, e);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetThrowsException() throws Exception {
    Exception error = new Exception();
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenThrow(new StoreAccessException(error));

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    try {
      tieredStore.get(1);
      fail("We should get an Error");
    } catch (RuntimeException e) {
      assertSame(error, e.getCause());
      assertEquals("Unexpected checked exception wrapped in StoreAccessException", e.getMessage());
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetThrowsPassthrough() throws Exception {
    StoreAccessException error = new StoreAccessException("inner");
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenThrow(new StoreAccessException(new StorePassThroughException(error)));

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    try {
      tieredStore.get(1);
      fail("We should get an Error");
    } catch (StoreAccessException e) {
      assertSame(error, e);
    }
  }

  @Test
  public void testPut() throws Exception {
    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    tieredStore.put(1, "one");

    verify(numberCachingTier, times(1)).invalidate(eq(1));
    verify(numberAuthoritativeTier, times(1)).put(eq(1), eq("one"));
  }

  @Test
  public void testPutIfAbsent_whenAbsent() throws Exception {
    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.putIfAbsent(1, "one", b -> {}), is(nullValue()));

    verify(numberCachingTier, times(1)).invalidate(eq(1));
    verify(numberAuthoritativeTier, times(1)).putIfAbsent(eq(1), eq("one"), any());
  }

  @Test
  public void testPutIfAbsent_whenPresent() throws Exception {
    Consumer<Boolean> booleanConsumer = b -> {
    };
    when(numberAuthoritativeTier.putIfAbsent(1, "one", booleanConsumer)).thenReturn(newValueHolder("un"));

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.putIfAbsent(1, "one", booleanConsumer).get(), Matchers.<CharSequence>equalTo("un"));

    verify(numberCachingTier, times(1)).invalidate(1);
    verify(numberAuthoritativeTier, times(1)).putIfAbsent(1, "one", booleanConsumer);
  }

  @Test
  public void testRemove() throws Exception {
    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    tieredStore.remove(1);

    verify(numberCachingTier, times(1)).invalidate(eq(1));
    verify(numberAuthoritativeTier, times(1)).remove(eq(1));
  }

  @Test
  public void testRemove2Args_removes() throws Exception {
    when(numberAuthoritativeTier.remove(eq(1), eq("one"))).thenReturn(RemoveStatus.REMOVED);

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.remove(1, "one"), is(RemoveStatus.REMOVED));

    verify(numberCachingTier, times(1)).invalidate(eq(1));
    verify(numberAuthoritativeTier, times(1)).remove(eq(1), eq("one"));
  }

  @Test
  public void testRemove2Args_doesNotRemove() throws Exception {
    when(numberAuthoritativeTier.remove(eq(1), eq("one"))).thenReturn(RemoveStatus.KEY_MISSING);

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.remove(1, "one"), is(RemoveStatus.KEY_MISSING));

    verify(numberCachingTier).invalidate(any(Number.class));
    verify(numberAuthoritativeTier, times(1)).remove(eq(1), eq("one"));
  }

  @Test
  public void testReplace2Args_replaces() throws Exception {
    when(numberAuthoritativeTier.replace(eq(1), eq("one"))).thenReturn(newValueHolder("un"));

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.replace(1, "one").get(), Matchers.<CharSequence>equalTo("un"));

    verify(numberCachingTier, times(1)).invalidate(eq(1));
    verify(numberAuthoritativeTier, times(1)).replace(eq(1), eq("one"));
  }

  @Test
  public void testReplace2Args_doesNotReplace() throws Exception {
    when(numberAuthoritativeTier.replace(eq(1), eq("one"))).thenReturn(null);

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.replace(1, "one"), is(nullValue()));

    verify(numberCachingTier).invalidate(any(Number.class));
    verify(numberAuthoritativeTier, times(1)).replace(eq(1), eq("one"));
  }

  @Test
  public void testReplace3Args_replaces() throws Exception {
    when(numberAuthoritativeTier.replace(eq(1), eq("un"), eq("one"))).thenReturn(ReplaceStatus.HIT);

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.replace(1, "un", "one"), is(ReplaceStatus.HIT));

    verify(numberCachingTier, times(1)).invalidate(eq(1));
    verify(numberAuthoritativeTier, times(1)).replace(eq(1), eq("un"), eq("one"));
  }

  @Test
  public void testReplace3Args_doesNotReplace() throws Exception {
    when(numberAuthoritativeTier.replace(eq(1), eq("un"), eq("one"))).thenReturn(ReplaceStatus.MISS_NOT_PRESENT);

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.replace(1, "un", "one"), is(ReplaceStatus.MISS_NOT_PRESENT));

    verify(numberCachingTier).invalidate(any(Number.class));
    verify(numberAuthoritativeTier, times(1)).replace(eq(1), eq("un"), eq("one"));
  }

  @Test
  public void testClear() throws Exception {
    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    tieredStore.clear();

    verify(numberCachingTier, times(1)).clear();
    verify(numberAuthoritativeTier, times(1)).clear();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompute2Args() throws Exception {
    when(numberAuthoritativeTier.getAndCompute(any(Number.class), any(BiFunction.class))).then((Answer<Store.ValueHolder<CharSequence>>) invocation -> {
      Number key = (Number) invocation.getArguments()[0];
      BiFunction<Number, CharSequence, CharSequence> function = (BiFunction<Number, CharSequence, CharSequence>) invocation.getArguments()[1];
      return newValueHolder(function.apply(key, null));
    });

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.getAndCompute(1, (number, charSequence) -> "one").get(), Matchers.<CharSequence>equalTo("one"));

    verify(numberCachingTier, times(1)).invalidate(any(Number.class));
    verify(numberAuthoritativeTier, times(1)).getAndCompute(eq(1), any(BiFunction.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCompute3Args() throws Exception {
    when(numberAuthoritativeTier.computeAndGet(any(Number.class), any(BiFunction.class), any(Supplier.class), any(Supplier.class))).then((Answer<Store.ValueHolder<CharSequence>>) invocation -> {
      Number key = (Number) invocation.getArguments()[0];
      BiFunction<Number, CharSequence, CharSequence> function = (BiFunction<Number, CharSequence, CharSequence>) invocation.getArguments()[1];
      return newValueHolder(function.apply(key, null));
    });

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.computeAndGet(1, (number, charSequence) -> "one", () -> true, () -> false).get(), Matchers.<CharSequence>equalTo("one"));

    verify(numberCachingTier, times(1)).invalidate(any(Number.class));
    verify(numberAuthoritativeTier, times(1)).computeAndGet(eq(1), any(BiFunction.class), any(Supplier.class), any(Supplier.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testComputeIfAbsent_computes() throws Exception {
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenAnswer((Answer<Store.ValueHolder<CharSequence>>) invocation -> {
      Number key = (Number) invocation.getArguments()[0];
      Function<Number, Store.ValueHolder<CharSequence>> function = (Function<Number, Store.ValueHolder<CharSequence>>) invocation.getArguments()[1];
      return function.apply(key);
    });
    when(numberAuthoritativeTier.computeIfAbsentAndFault(any(Number.class), any(Function.class))).thenAnswer((Answer<Store.ValueHolder<CharSequence>>) invocation -> {
      Number key = (Number) invocation.getArguments()[0];
      Function<Number, CharSequence> function = (Function<Number, CharSequence>) invocation.getArguments()[1];
      return newValueHolder(function.apply(key));
    });

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.computeIfAbsent(1, number -> "one").get(), Matchers.<CharSequence>equalTo("one"));

    verify(numberCachingTier, times(1)).getOrComputeIfAbsent(eq(1), any(Function.class));
    verify(numberAuthoritativeTier, times(1)).computeIfAbsentAndFault(eq(1), any(Function.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testComputeIfAbsent_doesNotCompute() throws Exception {
    final Store.ValueHolder<CharSequence> valueHolder = newValueHolder("one");
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenAnswer((Answer<Store.ValueHolder<CharSequence>>) invocation -> valueHolder);

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    assertThat(tieredStore.computeIfAbsent(1, number -> "one").get(), Matchers.<CharSequence>equalTo("one"));

    verify(numberCachingTier, times(1)).getOrComputeIfAbsent(eq(1), any(Function.class));
    verify(numberAuthoritativeTier, times(0)).computeIfAbsentAndFault(eq(1), any(Function.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testComputeIfAbsentThrowsError() throws Exception {
    Error error = new Error();
    when(numberCachingTier.getOrComputeIfAbsent(any(Number.class), any(Function.class))).thenThrow(new StoreAccessException(error));

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    try {
      tieredStore.computeIfAbsent(1, n -> null);
      fail("We should get an Error");
    } catch (Error e) {
      assertSame(error, e);
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBulkCompute2Args() throws Exception {
    when(numberAuthoritativeTier.bulkCompute(any(Set.class), any(Function.class))).thenAnswer((Answer<Map<Number, Store.ValueHolder<CharSequence>>>) invocation -> {
      Set<Number> keys = (Set) invocation.getArguments()[0];
      Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>> function = (Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>) invocation.getArguments()[1];

      List<Map.Entry<? extends Number, ? extends CharSequence>> functionArg = new ArrayList<>();
      for (Number key : keys) {
        functionArg.add(newMapEntry(key, null));
      }

      Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> functionResult = function.apply(functionArg);

      Map<Number, Store.ValueHolder<CharSequence>> result = new HashMap<>();
      for (Map.Entry<? extends Number, ? extends CharSequence> entry : functionResult) {
       result.put(entry.getKey(), newValueHolder(entry.getValue()));
      }

      return result;
    });

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    Map<Number, Store.ValueHolder<CharSequence>> result = tieredStore.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3)), entries -> new ArrayList<>(Arrays
      .asList(newMapEntry(1, "one"), newMapEntry(2, "two"), newMapEntry(3, "three"))));

    assertThat(result.size(), is(3));
    assertThat(result.get(1).get(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).get(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).get(), Matchers.<CharSequence>equalTo("three"));

    verify(numberCachingTier, times(1)).invalidate(1);
    verify(numberCachingTier, times(1)).invalidate(2);
    verify(numberCachingTier, times(1)).invalidate(3);
    verify(numberAuthoritativeTier, times(1)).bulkCompute(any(Set.class), any(Function.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBulkCompute3Args() throws Exception {
    when(
        numberAuthoritativeTier.bulkCompute(any(Set.class), any(Function.class), any(Supplier.class))).thenAnswer((Answer<Map<Number, Store.ValueHolder<CharSequence>>>) invocation -> {
          Set<Number> keys = (Set) invocation.getArguments()[0];
          Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>> function = (Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>) invocation.getArguments()[1];

          List<Map.Entry<? extends Number, ? extends CharSequence>> functionArg = new ArrayList<>();
          for (Number key : keys) {
            functionArg.add(newMapEntry(key, null));
          }

          Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> functionResult = function.apply(functionArg);

          Map<Number, Store.ValueHolder<CharSequence>> result = new HashMap<>();
          for (Map.Entry<? extends Number, ? extends CharSequence> entry : functionResult) {
            result.put(entry.getKey(), newValueHolder(entry.getValue()));
          }

          return result;
        });

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);

    Map<Number, Store.ValueHolder<CharSequence>> result = tieredStore.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3)), entries -> new ArrayList<>(Arrays
      .asList(newMapEntry(1, "one"), newMapEntry(2, "two"), newMapEntry(3, "three"))), () -> true);

    assertThat(result.size(), is(3));
    assertThat(result.get(1).get(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).get(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).get(), Matchers.<CharSequence>equalTo("three"));

    verify(numberCachingTier, times(1)).invalidate(1);
    verify(numberCachingTier, times(1)).invalidate(2);
    verify(numberCachingTier, times(1)).invalidate(3);
    verify(numberAuthoritativeTier, times(1)).bulkCompute(any(Set.class), any(Function.class), any(Supplier.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBulkComputeIfAbsent() throws Exception {
    when(numberAuthoritativeTier.bulkComputeIfAbsent(any(Set.class), any(Function.class))).thenAnswer((Answer<Map<Number, Store.ValueHolder<CharSequence>>>) invocation -> {
      Set<Number> keys = (Set) invocation.getArguments()[0];
      Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>> function = (Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>) invocation.getArguments()[1];

      List<Map.Entry<? extends Number, ? extends CharSequence>> functionArg = new ArrayList<>();
      for (Number key : keys) {
        functionArg.add(newMapEntry(key, null));
      }

      Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> functionResult = function.apply(functionArg);

      Map<Number, Store.ValueHolder<CharSequence>> result = new HashMap<>();
      for (Map.Entry<? extends Number, ? extends CharSequence> entry : functionResult) {
        result.put(entry.getKey(), newValueHolder(entry.getValue()));
      }

      return result;
    });

    TieredStore<Number, CharSequence> tieredStore = new TieredStore<>(numberCachingTier, numberAuthoritativeTier);


    Map<Number, Store.ValueHolder<CharSequence>> result = tieredStore.bulkComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3)), numbers -> Arrays.asList(newMapEntry(1, "one"), newMapEntry(2, "two"), newMapEntry(3, "three")));

    assertThat(result.size(), is(3));
    assertThat(result.get(1).get(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).get(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).get(), Matchers.<CharSequence>equalTo("three"));

    verify(numberCachingTier, times(1)).invalidate(1);
    verify(numberCachingTier, times(1)).invalidate(2);
    verify(numberCachingTier, times(1)).invalidate(3);
    verify(numberAuthoritativeTier, times(1)).bulkComputeIfAbsent(any(Set.class), any(Function.class));
  }

  @Test
  public void CachingTierDoesNotSeeAnyOperationDuringClear() throws StoreAccessException, BrokenBarrierException, InterruptedException {
    final TieredStore<String, String> tieredStore = new TieredStore<>(stringCachingTier, stringAuthoritativeTier);

    final CyclicBarrier barrier = new CyclicBarrier(2);

    doAnswer((Answer<Void>) invocation -> {
      barrier.await();
      barrier.await();
      return null;
    }).when(stringAuthoritativeTier).clear();
    Thread t = new Thread(() -> {
      try {
        tieredStore.clear();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    t.start();
    barrier.await();
    tieredStore.get("foo");
    barrier.await();
    t.join();
    verify(stringCachingTier, never()).getOrComputeIfAbsent(
      ArgumentMatchers.any(), ArgumentMatchers.any());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testReleaseStoreFlushes() throws Exception {
    TieredStore.Provider tieredStoreProvider = new TieredStore.Provider();

    ResourcePools resourcePools = mock(ResourcePools.class);
    when(resourcePools.getResourceTypeSet())
        .thenReturn(new HashSet<>(Arrays.asList(ResourceType.Core.HEAP, ResourceType.Core.OFFHEAP)));

    SizedResourcePool heapPool = mock(SizedResourcePool.class);
    when(heapPool.getType()).thenReturn((ResourceType)ResourceType.Core.HEAP);
    when(resourcePools.getPoolForResource(ResourceType.Core.HEAP)).thenReturn(heapPool);
    OnHeapStore.Provider onHeapStoreProvider = mock(OnHeapStore.Provider.class);
    Set<ResourceType<?>> singleton = Collections.<ResourceType<?>>singleton( ResourceType.Core.HEAP);
    when(onHeapStoreProvider.rankCachingTier(eq(singleton), any(Collection.class))).thenReturn(1);
    when(onHeapStoreProvider.createCachingTier(any(Store.Configuration.class),
      ArgumentMatchers.any()))
        .thenReturn(stringCachingTier);

    SizedResourcePool offHeapPool = mock(SizedResourcePool.class);
    when(heapPool.getType()).thenReturn((ResourceType)ResourceType.Core.OFFHEAP);
    when(resourcePools.getPoolForResource(ResourceType.Core.OFFHEAP)).thenReturn(offHeapPool);
    OffHeapStore.Provider offHeapStoreProvider = mock(OffHeapStore.Provider.class);
    when(offHeapStoreProvider.rankAuthority(eq(ResourceType.Core.OFFHEAP), any(Collection.class))).thenReturn(1);
    when(offHeapStoreProvider.createAuthoritativeTier(
            any(Store.Configuration.class), ArgumentMatchers.any()))
        .thenReturn(stringAuthoritativeTier);

    Store.Configuration<String, String> configuration = mock(Store.Configuration.class);
    when(configuration.getResourcePools()).thenReturn(resourcePools);

    Set<AuthoritativeTier.Provider> authorities = new HashSet<>();
    authorities.add(offHeapStoreProvider);
    Set<CachingTier.Provider> cachingTiers = new HashSet<>();
    cachingTiers.add(onHeapStoreProvider);
    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    when(serviceProvider.getService(OnHeapStore.Provider.class)).thenReturn(onHeapStoreProvider);
    when(serviceProvider.getService(OffHeapStore.Provider.class)).thenReturn(offHeapStoreProvider);
    when(serviceProvider.getServicesOfType(AuthoritativeTier.Provider.class)).thenReturn(authorities);
    when(serviceProvider.getServicesOfType(CachingTier.Provider.class)).thenReturn(cachingTiers);
    tieredStoreProvider.start(serviceProvider);

    final Store<String, String> tieredStore = tieredStoreProvider.createStore(configuration);
    tieredStoreProvider.initStore(tieredStore);
    tieredStoreProvider.releaseStore(tieredStore);
    verify(onHeapStoreProvider, times(1)).releaseCachingTier(any(CachingTier.class));
  }

  @Test
  public void testRank() throws Exception {
    TieredStore.Provider provider = new TieredStore.Provider();
    ServiceLocator serviceLocator = dependencySet().with(provider).with(mock(DiskResourceService.class)).build();
    serviceLocator.startAllServices();

    assertRank(provider, 0, ResourceType.Core.DISK);
    assertRank(provider, 0, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.OFFHEAP);
    assertRank(provider, 2, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);
    assertRank(provider, 2, ResourceType.Core.DISK, ResourceType.Core.HEAP);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP);
    assertRank(provider, 3, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP);

    final ResourceType<ResourcePool> unmatchedResourceType = new ResourceType<ResourcePool>() {
      @Override
      public Class<ResourcePool> getResourcePoolClass() {
        return ResourcePool.class;
      }
      @Override
      public boolean isPersistable() {
        return true;
      }
      @Override
      public boolean requiresSerialization() {
        return true;
      }
      @Override
      public int getTierHeight() {
        return 10;
      }
    };
    assertRank(provider, 0, unmatchedResourceType);
    assertRank(provider, 0, ResourceType.Core.DISK, ResourceType.Core.OFFHEAP, ResourceType.Core.HEAP, unmatchedResourceType);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetAuthoritativeTierProvider() {
    TieredStore.Provider provider = new TieredStore.Provider();
    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    provider.start(serviceProvider);

    AuthoritativeTier.Provider provider1 = mock(AuthoritativeTier.Provider.class);
    when(provider1.rankAuthority(any(ResourceType.class), any())).thenReturn(1);
    AuthoritativeTier.Provider provider2 = mock(AuthoritativeTier.Provider.class);
    when(provider2.rankAuthority(any(ResourceType.class), any())).thenReturn(2);

    when(serviceProvider.getServicesOfType(AuthoritativeTier.Provider.class)).thenReturn(Arrays.asList(provider1,
                                                                                                       provider2));

    assertSame(provider.getAuthoritativeTierProvider(mock(ResourceType.class), Collections.emptyList()), provider2);
  }

  private void assertRank(final Store.Provider provider, final int expectedRank, final ResourceType<?>... resources) {
    Assert.assertThat(provider.rank(
      new HashSet<>(Arrays.asList(resources)),
        Collections.emptyList()),
        Matchers.is(expectedRank));
  }

  public Map.Entry<? extends Number, ? extends CharSequence> newMapEntry(Number key, CharSequence value) {
    return new AbstractMap.SimpleEntry<>(key, value);
  }

  public Store.ValueHolder<CharSequence> newValueHolder(final CharSequence v) {
    return new Store.ValueHolder<CharSequence>() {

      @Override
      public CharSequence get() {
        return v;
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
        throw new UnsupportedOperationException("Implement me!");
      }
    };
  }

}
