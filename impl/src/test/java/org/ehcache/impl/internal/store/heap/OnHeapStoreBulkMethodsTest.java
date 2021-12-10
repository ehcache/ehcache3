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

import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.store.Store;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
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
    return new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE, IdentityCopier.identityCopier(), IdentityCopier.identityCopier(),
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

    OnHeapStore<Number, Number> store = new OnHeapStore<>(config, SystemTimeSource.INSTANCE, IdentityCopier.identityCopier(), IdentityCopier.identityCopier(),
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
    Store.Configuration<Number, CharSequence> configuration = mockStoreConfig();

    @SuppressWarnings("unchecked")
    OnHeapStore<Number, CharSequence> store = new OnHeapStore<>(configuration, SystemTimeSource.INSTANCE, IdentityCopier.identityCopier(),
      IdentityCopier.identityCopier(), new NoopSizeOfEngine(), NullStoreEventDispatcher.nullStoreEventDispatcher(), new DefaultStatisticsService());
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

}
