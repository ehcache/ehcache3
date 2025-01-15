/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.core.internal.statistics.DefaultStatisticsService;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.store.Store;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class OnHeapStoreBulkMethodsTest {

  @SuppressWarnings("unchecked")
  protected <K, V> Store.Configuration<K, V> mockStoreConfig() {
    @SuppressWarnings("rawtypes")
    Store.Configuration config = mock(Store.Configuration.class);
    when(config.getExpiry()).thenReturn(ExpiryPolicyBuilder.noExpiration());
    when(config.getKeyType()).thenReturn(Number.class);
    when(config.getValueType()).thenReturn(CharSequence.class);
    when(config.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build());
    return config;
  }

  @SuppressWarnings("unchecked")
  protected <Number, CharSequence> OnHeapStore<Number, CharSequence> newStore() {
    Store.Configuration<Number, CharSequence> configuration = mockStoreConfig();
    return new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE,
        new NoopSizeOfEngine(), NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBulkComputeFunctionGetsValuesOfEntries() throws Exception {
    @SuppressWarnings("rawtypes")
    Store.Configuration config = mock(Store.Configuration.class);
    when(config.getExpiry()).thenReturn(ExpiryPolicyBuilder.noExpiration());
    when(config.getKeyType()).thenReturn(Number.class);
    when(config.getValueType()).thenReturn(Number.class);
    when(config.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build());

    OnHeapStore<Number, Number> store = new OnHeapStore<>(config, SystemTimeSource.INSTANCE,
        new NoopSizeOfEngine(), NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());
    store.put(1, 2);
    store.put(2, 3);
    store.put(3, 4);

    Map<Number, Store.ValueHolder<Number>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5, 6)), entries -> {
      Map<Number, Number> newValues = new HashMap<>();
      for (Map.Entry<? extends Number, ? extends Number> entry : entries) {
        final Number currentValue = entry.getValue();
        if(currentValue == null) {
          if(entry.getKey().equals(4)) {
            newValues.put(entry.getKey(), null);
          } else {
            newValues.put(entry.getKey(), 0);
          }
        } else {
          newValues.put(entry.getKey(), currentValue.intValue() * 2);
        }

      }
      return newValues.entrySet();
    });

    ConcurrentMap<Number, Number> check = new ConcurrentHashMap<>();
    check.put(1, 4);
    check.put(2, 6);
    check.put(3, 8);
    check.put(4, 0);
    check.put(5, 0);
    check.put(6, 0);

    assertThat(result.get(1).get(), Matchers.is(check.get(1)));
    assertThat(result.get(2).get(), Matchers.is(check.get(2)));
    assertThat(result.get(3).get(), Matchers.is(check.get(3)));
    assertThat(result.get(4), nullValue());
    assertThat(result.get(5).get(), Matchers.is(check.get(5)));
    assertThat(result.get(6).get(), Matchers.is(check.get(6)));

    for (Number key : check.keySet()) {
      final Store.ValueHolder<Number> holder = store.get(key);
      if(holder != null) {
        check.remove(key, holder.get());
      }
    }
    assertThat(check.size(), is(1));
    assertThat(check.containsKey(4), is(true));

  }

  @Test
  public void testBulkComputeHappyPath() throws Exception {
    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2)), entries -> {
      Map<Number, CharSequence> newValues = new HashMap<>();
      for (Map.Entry<? extends Number, ? extends CharSequence> entry : entries) {
        if(entry.getKey().intValue() == 1) {
          newValues.put(entry.getKey(), "un");
        } else if (entry.getKey().intValue() == 2)  {
          newValues.put(entry.getKey(), "deux");
        }
      }
      return newValues.entrySet();
    });

    assertThat(result.size(), is(2));
    assertThat(result.get(1).get(), Matchers.equalTo("un"));
    assertThat(result.get(2).get(), Matchers.equalTo("deux"));

    assertThat(store.get(1).get(), Matchers.equalTo("un"));
    assertThat(store.get(2).get(), Matchers.equalTo("deux"));
  }

  @Test
  public void testBulkComputeStoreRemovesValueWhenFunctionReturnsNullMappings() throws Exception {
    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(2, 1, 5)), entries -> {
      Map<Number, CharSequence> newValues = new HashMap<>();
      for (Map.Entry<? extends Number, ? extends CharSequence> entry : entries) {
        newValues.put(entry.getKey(), null);
      }
      return newValues.entrySet();
    });

    assertThat(result.size(), is(3));

    assertThat(store.get(1), is(nullValue()));
    assertThat(store.get(2), is(nullValue()));
    assertThat(store.get(3).get(), Matchers.equalTo("three"));
    assertThat(store.get(5), is(nullValue()));
  }

  @Test
  public void testBulkComputeRemoveNullValueEntriesFromFunctionReturn() throws Exception {

    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3)), entries -> {
      Map<Number, CharSequence> result1 = new HashMap<>();
      for (Map.Entry<? extends Number, ? extends CharSequence> entry : entries) {
        if (entry.getKey().equals(1)) {
          result1.put(entry.getKey(), null);
        } else if (entry.getKey().equals(3)) {
          result1.put(entry.getKey(), null);
        } else {
          result1.put(entry.getKey(), entry.getValue());
        }
      }
      return result1.entrySet();
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(1), is(nullValue()));
    assertThat(result.get(2).get(), Matchers.equalTo("two"));
    assertThat(result.get(3), is(nullValue()));

    assertThat(store.get(1),is(nullValue()));
    assertThat(store.get(2).get(), Matchers.equalTo("two"));
    assertThat(store.get(3),is(nullValue()));

  }

  @Test
  public void testBulkComputeIfAbsentFunctionDoesNotGetPresentKeys() throws Exception {

    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5, 6)), keys -> {
      Map<Number, CharSequence> result1 = new HashMap<>();

      for (Number key : keys) {
        if (key.equals(1)) {
          fail();
        } else if (key.equals(2)) {
          fail();
        } else if (key.equals(3)) {
          fail();
        } else {
          result1.put(key, null);
        }
      }
      return result1.entrySet();
    });

    assertThat(result.size(), is(6));
    assertThat(result.get(1).get(), Matchers.equalTo("one"));
    assertThat(result.get(2).get(), Matchers.equalTo("two"));
    assertThat(result.get(3).get(), Matchers.equalTo("three"));
    assertThat(result.get(4), is(nullValue()));
    assertThat(result.get(5), is(nullValue()));
    assertThat(result.get(6), is(nullValue()));

    assertThat(store.get(1).get(), Matchers.equalTo("one"));
    assertThat(store.get(2).get(), Matchers.equalTo("two"));
    assertThat(store.get(3).get(), Matchers.equalTo("three"));
    assertThat(store.get(4), is(nullValue()));
    assertThat(store.get(5), is(nullValue()));
    assertThat(store.get(6), is(nullValue()));


  }

  @Test
  public void testBulkComputeIfAbsentDoesNotOverridePresentKeys() throws Exception {

    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5, 6)), numbers -> {
      Map<Number, CharSequence> result1 = new HashMap<>();
      for (Number key : numbers) {
        if(key.equals(4)) {
          result1.put(key, "quatre");
        } else if(key.equals(5)) {
          result1.put(key, "cinq");
        } else if(key.equals(6)) {
          result1.put(key, "six");
        }
      }
      return result1.entrySet();
    });

    assertThat(result.size(), is(6));
    assertThat(result.get(1).get(), Matchers.equalTo("one"));
    assertThat(result.get(2).get(), Matchers.equalTo("two"));
    assertThat(result.get(3).get(), Matchers.equalTo("three"));
    assertThat(result.get(4).get(), Matchers.equalTo("quatre"));
    assertThat(result.get(5).get(), Matchers.equalTo("cinq"));
    assertThat(result.get(6).get(), Matchers.equalTo("six"));

    assertThat(store.get(1).get(), Matchers.equalTo("one"));
    assertThat(store.get(2).get(), Matchers.equalTo("two"));
    assertThat(store.get(3).get(), Matchers.equalTo("three"));
    assertThat(store.get(4).get(), Matchers.equalTo("quatre"));
    assertThat(store.get(5).get(), Matchers.equalTo("cinq"));
    assertThat(store.get(6).get(), Matchers.equalTo("six"));
  }

  @Test
  public void testBulkComputeIfAbsentDoNothingOnNullValues() throws Exception {

    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(new HashSet<Number>(Arrays.asList(2, 1, 5)), numbers -> {
      Map<Number, CharSequence> result1 = new HashMap<>();
      for (Number key : numbers) {
        // 5 is a missing key, so it's the only key that is going passed to the function
        if(key.equals(5)) {
          result1.put(key, null);
        }
      }
      Set<Number> numbersSet = new HashSet<>();
      for (Number number : numbers) {
        numbersSet.add(number);
      }
      assertThat(numbersSet.size(), is(1));
      assertThat(numbersSet.iterator().next(), Matchers.<Number>equalTo(5));

      return result1.entrySet();
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(2).get(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(1).get(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(5), is(nullValue()));

    assertThat(store.get(1).get(), Matchers.<CharSequence>equalTo("one"));
    assertThat(store.get(2).get(), Matchers.<CharSequence>equalTo("two"));
    assertThat(store.get(3).get(), Matchers.<CharSequence>equalTo("three"));
    assertThat(store.get(5), is(nullValue()));
  }


  @Test
  public void testBulkGetOrComputeIfAbsentHappyPath() throws Exception {
    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkGetOrComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3)), missingKeys -> Collections.emptyList());

    assertThat(result.size(), is(2));
    assertThat(result.get(1).get(), Matchers.equalTo("one"));
    assertThat(result.get(2).get(), Matchers.equalTo("two"));
  }

  @Test
  public void testBulkGetOrComputeIfAbsent_compute() throws Exception {
    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");

    Function<Set<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends Store.ValueHolder<CharSequence>>>> mappingFunction =
      missingKeys -> Arrays.asList(newMapEntry(1, "updated-one"), newMapEntry(2, "two"), newMapEntry(3, "three"));

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkGetOrComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3, 4)), mappingFunction);
    assertThat(result.size(), is(3));
    assertThat(result.get(1).get(), Matchers.equalTo("one"));
    assertThat(result.get(2).get(), Matchers.equalTo("two"));
    assertThat(result.get(3).get(), Matchers.equalTo("three"));
    assertThat(store.get(1).get(), Matchers.equalTo("one"));
    assertThat(store.get(2).get(), Matchers.equalTo("two"));
    assertThat(store.get(3).get(), Matchers.equalTo("three"));
  }

  @Test
  public void testBulkGetOrComputeIfAbsent_WhenFunctionReturnsNullMappings() throws Exception {
    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");

    Function<Set<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends Store.ValueHolder<CharSequence>>>> mappingFunction =
      missingKeys -> Arrays.asList(newMapEntry(2, "two"), newMapEntry(3, null), newMapEntry(4, null));

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkGetOrComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5)), mappingFunction);
    assertThat(result.size(), is(4));
    assertThat(result.get(1).get(), Matchers.equalTo("one"));
    assertThat(result.get(2).get(), Matchers.equalTo("two"));
    assertThat(result.get(3), is(nullValue()));
    assertThat(result.get(4), is(nullValue()));
    assertThat(store.get(2).get(), Matchers.equalTo("two"));
    assertThat(store.get(3), is(nullValue()));
    assertThat(store.get(4), is(nullValue()));
  }

  @Test
  public void testBulkGetOrComputeIfAbsent_DoesNotGetPresentKeys() throws Exception {

    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");


    Function<Set<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends Store.ValueHolder<CharSequence>>>> mappingFunction =
      missingKeys -> {
        List<Map.Entry<? extends Number, ? extends Store.ValueHolder<CharSequence>>> missingMappings = new ArrayList<>();
        for (Number key : missingKeys) {
          if (key.equals(1) || key.equals(2) || key.equals(3)) {
            fail();
          } else {
            missingMappings.add(newMapEntry(key, "default"));
          }
        }
        return missingMappings;
      };

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkGetOrComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5, 6)), mappingFunction);
    assertThat(result.size(), is(6));
    assertThat(result.get(1).get(), Matchers.equalTo("one"));
    assertThat(result.get(2).get(), Matchers.equalTo("two"));
    assertThat(result.get(3).get(), Matchers.equalTo("three"));
    assertThat(result.get(4).get(), Matchers.equalTo("default"));
    assertThat(result.get(5).get(), Matchers.equalTo("default"));
    assertThat(result.get(6).get(), Matchers.equalTo("default"));

    assertThat(store.get(1).get(), Matchers.equalTo("one"));
    assertThat(store.get(2).get(), Matchers.equalTo("two"));
    assertThat(store.get(3).get(), Matchers.equalTo("three"));
    assertThat(store.get(4).get(), Matchers.equalTo("default"));
    assertThat(store.get(5).get(), Matchers.equalTo("default"));
    assertThat(store.get(6).get(), Matchers.equalTo("default"));
  }

  public Map.Entry<? extends Number, ? extends Store.ValueHolder<CharSequence>> newMapEntry(Number key, CharSequence value) {
    return new AbstractMap.SimpleEntry<>(key, value == null ? null : newValueHolder(value));
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
        return 0;
      }
    };
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCachingTierEviction_fetchHeapTierOnly_NoEviction() throws Exception {
    OnHeapStore<Number, CharSequence> spyStore = Mockito.spy(newStore());
    for (int i = 1; i <= 5; i++) {
      spyStore.put(i, "val" + i);
    }

    Set<Number> keys = new HashSet<>(Arrays.asList(1, 2, 3, 4, 5));
    Map<Number, Store.ValueHolder<CharSequence>> getAllResult = spyStore.bulkGetOrComputeIfAbsent(keys,
      missingKeys -> Collections.emptyList());

    assertThat(getAllResult.size(), Matchers.is(5));
    verify(spyStore, times(0)).getOrComputeIfAbsent(any(Number.class), any(Function.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCachingTierEviction_fetchHeapTierAndMappingFunction() throws Exception {
    OnHeapStore<Number, CharSequence> spyStore = Mockito.spy(newStore());
    for (int i = 1; i <= 5; i++) {
      spyStore.put(i, "val" + i);
    }

    Set<Number> keys = new HashSet<>(Arrays.asList(3, 4, 5, 6, 7));
    Map<Number, Store.ValueHolder<CharSequence>> getAllResult = spyStore.bulkGetOrComputeIfAbsent(keys,
      missingKeys -> Arrays.asList(newMapEntry(6, "val6"), newMapEntry(7, null)));

    assertThat(getAllResult.size(), Matchers.is(5));
    verify(spyStore, times(2)).getOrComputeIfAbsent(any(Number.class), any(Function.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCachingTierEviction_fetchMappingFunctionOnly_heapFull() throws Exception {
    OnHeapStore<Number, CharSequence> spyStore = Mockito.spy(newStore());
    for (int i = 1; i <= 5; i++) {
      spyStore.put(i, "val" + i);
    }

    List<Map.Entry<? extends Number, ? extends Store.ValueHolder<CharSequence>>> funMapping = new ArrayList<>();
    Set<Number> keys = new HashSet<>();
    for (int i = 6; i <= 10; i++) {
      keys.add(i);
      funMapping.add(newMapEntry(i, "val" + i));
    }
    Map<Number, Store.ValueHolder<CharSequence>> getAllResult = spyStore.bulkGetOrComputeIfAbsent(keys, missingKeys -> funMapping);

    assertThat(getAllResult.size(), Matchers.is(5));
    verify(spyStore, times(5)).getOrComputeIfAbsent(any(Number.class), any(Function.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCachingTierEviction_fetchMappingFunctionOnly_heapEmpty() throws Exception {
    OnHeapStore<Number, CharSequence> spyStore = Mockito.spy(newStore());
    List<Map.Entry<? extends Number, ? extends Store.ValueHolder<CharSequence>>> funMapping = new ArrayList<>();
    Set<Number> keys = new HashSet<>();
    for (int i = 1; i <= 5; i++) {
      keys.add(i);
      funMapping.add(newMapEntry(i, "val" + i));
    }
    Map<Number, Store.ValueHolder<CharSequence>> getAllResult = spyStore.bulkGetOrComputeIfAbsent(keys, missingKeys -> funMapping);
    assertThat(getAllResult.size(), Matchers.is(5));

    verify(spyStore, times(5)).getOrComputeIfAbsent(any(Number.class), any(Function.class));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCachingTierEviction_fetchMappingFunctionOnly_heapEmpty_exceedingHeapSize() throws Exception {
    @SuppressWarnings("rawtypes")
    Store.Configuration config = mock(Store.Configuration.class);
    when(config.getExpiry()).thenReturn(ExpiryPolicyBuilder.noExpiration());
    when(config.getKeyType()).thenReturn(Number.class);
    when(config.getValueType()).thenReturn(CharSequence.class);
    when(config.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(5, EntryUnit.ENTRIES).build());

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(config, SystemTimeSource.INSTANCE,
      new NoopSizeOfEngine(), NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());

    OnHeapStore<Number, CharSequence> spyStore = Mockito.spy(store);

    List<Map.Entry<? extends Number, ? extends Store.ValueHolder<CharSequence>>> funMapping = new ArrayList<>();
    Set<Number> keys = new HashSet<>();
    for (int i = 1; i <= 10; i++) {
      keys.add(i);
      funMapping.add(newMapEntry(i, "val" + i));
    }
    Map<Number, Store.ValueHolder<CharSequence>> getAllResult = spyStore.bulkGetOrComputeIfAbsent(keys, missingKeys -> funMapping);
    assertThat(getAllResult.size(), Matchers.is(10));

    verify(spyStore, times(5)).getOrComputeIfAbsent(any(Number.class), any(Function.class));
  }

}
