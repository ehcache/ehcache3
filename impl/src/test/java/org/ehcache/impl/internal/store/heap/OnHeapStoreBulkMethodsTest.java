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

import org.ehcache.config.units.EntryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.core.spi.function.Function;
import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.copy.Copier;
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

  public static final Copier DEFAULT_COPIER = new IdentityCopier();

  @SuppressWarnings("unchecked")
  protected <K, V> Store.Configuration<K, V> mockStoreConfig() {
    @SuppressWarnings("rawtypes")
    Store.Configuration config = mock(Store.Configuration.class);
    when(config.getExpiry()).thenReturn(Expirations.noExpiration());
    when(config.getKeyType()).thenReturn(Number.class);
    when(config.getValueType()).thenReturn(CharSequence.class);
    when(config.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build());
    return config;
  }

  @SuppressWarnings("unchecked")
  protected <Number, CharSequence> OnHeapStore<Number, CharSequence> newStore() {
    Store.Configuration<Number, CharSequence> configuration = mockStoreConfig();
    return new OnHeapStore<Number, CharSequence>(configuration, SystemTimeSource.INSTANCE, DEFAULT_COPIER, DEFAULT_COPIER,
        new NoopSizeOfEngine(), NullStoreEventDispatcher.<Number, CharSequence>nullStoreEventDispatcher());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBulkComputeFunctionGetsValuesOfEntries() throws Exception {
    @SuppressWarnings("rawtypes")
    Store.Configuration config = mock(Store.Configuration.class);
    when(config.getExpiry()).thenReturn(Expirations.noExpiration());
    when(config.getKeyType()).thenReturn(Number.class);
    when(config.getValueType()).thenReturn(Number.class);
    when(config.getResourcePools()).thenReturn(newResourcePoolsBuilder().heap(Long.MAX_VALUE, EntryUnit.ENTRIES).build());
    Store.Configuration<Number, Number> configuration = config;

    OnHeapStore<Number, Number> store = new OnHeapStore<Number, Number>(configuration, SystemTimeSource.INSTANCE, DEFAULT_COPIER, DEFAULT_COPIER,
        new NoopSizeOfEngine(), NullStoreEventDispatcher.<Number, Number>nullStoreEventDispatcher());
    store.put(1, 2);
    store.put(2, 3);
    store.put(3, 4);

    Map<Number, Store.ValueHolder<Number>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5, 6)), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends Number>>, Iterable<? extends Map.Entry<? extends Number, ? extends Number>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends Number>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends Number>> entries) {
        Map<Number, Number> newValues = new HashMap<Number, Number>();
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
      }
    });

    ConcurrentMap<Number, Number> check = new ConcurrentHashMap<Number, Number>();
    check.put(1, 4);
    check.put(2, 6);
    check.put(3, 8);
    check.put(4, 0);
    check.put(5, 0);
    check.put(6, 0);

    assertThat(result.get(1).value(), Matchers.<Number>is(check.get(1)));
    assertThat(result.get(2).value(), Matchers.<Number>is(check.get(2)));
    assertThat(result.get(3).value(), Matchers.<Number>is(check.get(3)));
    assertThat(result.get(4), nullValue());
    assertThat(result.get(5).value(), Matchers.<Number>is(check.get(5)));
    assertThat(result.get(6).value(), Matchers.<Number>is(check.get(6)));

    for (Number key : check.keySet()) {
      final Store.ValueHolder<Number> holder = store.get(key);
      if(holder != null) {
        check.remove(key, holder.value());
      }
    }
    assertThat(check.size(), is(1));
    assertThat(check.containsKey(4), is(true));

  }

  @Test
  public void testBulkComputeHappyPath() throws Exception {
    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2)), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        Map<Number, CharSequence> newValues = new HashMap<Number, CharSequence>();
        for (Map.Entry<? extends Number, ? extends CharSequence> entry : entries) {
          if(entry.getKey().intValue() == 1) {
            newValues.put(entry.getKey(), "un");
          } else if (entry.getKey().intValue() == 2)  {
            newValues.put(entry.getKey(), "deux");
          }
        }
        return newValues.entrySet();
      }
    });

    assertThat(result.size(), is(2));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("un"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("deux"));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("un"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("deux"));
  }

  @Test
  public void testBulkComputeStoreRemovesValueWhenFunctionReturnsNullMappings() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mockStoreConfig();

    @SuppressWarnings("unchecked")
    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration, SystemTimeSource.INSTANCE, DEFAULT_COPIER, DEFAULT_COPIER, new NoopSizeOfEngine(), NullStoreEventDispatcher.<Number, CharSequence>nullStoreEventDispatcher());
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(2, 1, 5)), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        Map<Number, CharSequence> newValues = new HashMap<Number, CharSequence>();
        for (Map.Entry<? extends Number, ? extends CharSequence> entry : entries) {
          newValues.put(entry.getKey(), null);
        }
        return newValues.entrySet();
      }
    });

    assertThat(result.size(), is(3));

    assertThat(store.get(1), is(nullValue()));
    assertThat(store.get(2), is(nullValue()));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(store.get(5), is(nullValue()));
  }

  @Test
  public void testBulkComputeRemoveNullValueEntriesFromFunctionReturn() throws Exception {

    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(new HashSet<Number>(Arrays.asList(1, 2, 3)), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        Map<Number, CharSequence> result = new HashMap<Number, CharSequence>();
        for (Map.Entry<? extends Number, ? extends CharSequence> entry : entries) {
          if (entry.getKey().equals(1)) {
            result.put(entry.getKey(), null);
          } else if (entry.getKey().equals(3)) {
            result.put(entry.getKey(), null);
          } else {
            result.put(entry.getKey(), entry.getValue());
          }
        }
        return result.entrySet();
      }
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(1), is(nullValue()));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3), is(nullValue()));

    assertThat(store.get(1),is(nullValue()));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(store.get(3),is(nullValue()));

  }

  @Test
  public void testBulkComputeIfAbsentFunctionDoesNotGetPresentKeys() throws Exception {

    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5, 6)), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> keys) {
        Map<Number, CharSequence> result = new HashMap<Number, CharSequence>();

        for (Number key : keys) {
          if (key.equals(1)) {
            fail();
          } else if (key.equals(2)) {
            fail();
          } else if (key.equals(3)) {
            fail();
          } else {
            result.put(key, null);
          }
        }
        return result.entrySet();
      }
    });

    assertThat(result.size(), is(6));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(result.get(4), is(nullValue()));
    assertThat(result.get(5), is(nullValue()));
    assertThat(result.get(6), is(nullValue()));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
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

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(new HashSet<Number>(Arrays.asList(1, 2, 3, 4, 5, 6)), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> numbers) {
        Map<Number, CharSequence> result = new HashMap<Number, CharSequence>();
        for (Number key : numbers) {
          if(key.equals(4)) {
            result.put(key, "quatre");
          } else if(key.equals(5)) {
            result.put(key, "cinq");
          } else if(key.equals(6)) {
            result.put(key, "six");
          }
        }
        return result.entrySet();
      }
    });

    assertThat(result.size(), is(6));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(result.get(4).value(), Matchers.<CharSequence>equalTo("quatre"));
    assertThat(result.get(5).value(), Matchers.<CharSequence>equalTo("cinq"));
    assertThat(result.get(6).value(), Matchers.<CharSequence>equalTo("six"));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(store.get(4).value(), Matchers.<CharSequence>equalTo("quatre"));
    assertThat(store.get(5).value(), Matchers.<CharSequence>equalTo("cinq"));
    assertThat(store.get(6).value(), Matchers.<CharSequence>equalTo("six"));
  }

  @Test
  public void testBulkComputeIfAbsentDoNothingOnNullValues() throws Exception {

    OnHeapStore<Number, CharSequence> store = newStore();
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(new HashSet<Number>(Arrays.asList(2, 1, 5)), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> numbers) {
        Map<Number, CharSequence> result = new HashMap<Number, CharSequence>();
        for (Number key : numbers) {
          // 5 is a missing key, so it's the only key that is going passed to the function
          if(key.equals(5)) {
            result.put(key, null);
          }
        }
        Set<Number> numbersSet =  new HashSet<Number>();
        for (Number number : numbers) {
          numbersSet.add(number);
        }
        assertThat(numbersSet.size(), is(1));
        assertThat(numbersSet.iterator().next(), Matchers.<Number>equalTo(5));

        return result.entrySet();
      }
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(5), is(nullValue()));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(store.get(5), is(nullValue()));
  }

}
