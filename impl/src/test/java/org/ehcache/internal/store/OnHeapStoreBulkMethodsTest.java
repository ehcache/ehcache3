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

package org.ehcache.internal.store;

import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class OnHeapStoreBulkMethodsTest {
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testBulkComputeFunctionWrongTypes() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);
    
    // raw type reference to store
    OnHeapStore store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    
    Set keys = new HashSet();
    keys.add(this); // wrong key type
    
    try {
      store.bulkCompute(keys, new Function() {
        @Override
        public Object apply(Object o) {
          throw new AssertionError();
        }
      });
      fail();
    } catch (ClassCastException e) { // expected
      assertThat(e.getMessage(), startsWith("Invalid key type"));
    }
 
    keys = new HashSet();
    keys.add(1);
    try {
      store.bulkCompute(keys, new Function() {
        @Override
        public Object apply(Object o) {
          return makeEntries(this, ""); // wrong key type
        }
      });
      fail();
    } catch (ClassCastException e) { // expected
      assertThat(e.getMessage(), startsWith("Invalid key type"));
    }
    
    keys = new HashSet();
    keys.add(1);
    try {
      store.bulkCompute(keys, new Function() {
        @Override
        public Object apply(Object o) {
          return makeEntries(1, this); // wrong value type
        }
      });
      fail();
    } catch (ClassCastException e) { // expected
      assertThat(e.getMessage(), startsWith("Invalid value type"));
    }
  }  
  
  @SuppressWarnings({ "unchecked", "rawtypes" })
  @Test
  public void testBulkComputeIfAbsentFunctionWrongTypes() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);
    
    // raw type reference to store
    OnHeapStore store = new OnHeapStore<Number, CharSequence>(configuration);
    
    Set keys = new HashSet();
    keys.add(this); // wrong key type
    
    try {
      store.bulkComputeIfAbsent(keys, new Function() {
        @Override
        public Object apply(Object o) {
          throw new AssertionError();
        }
      });
      fail();
    } catch (ClassCastException e) { // expected
      assertThat(e.getMessage(), startsWith("Invalid key type"));
    }
 
    keys = new HashSet();
    keys.add(1);
    try {
      store.bulkComputeIfAbsent(keys, new Function() {
        @Override
        public Object apply(Object o) {
          return makeEntries(this, ""); // wrong key type
        }
      });
      fail();
    } catch (ClassCastException e) { // expected
      assertThat(e.getMessage(), startsWith("Invalid key type"));
    }
    
    keys = new HashSet();
    keys.add(1);
    try {
      store.bulkComputeIfAbsent(keys, new Function() {
        @Override
        public Object apply(Object o) {
          return makeEntries(1, this); // wrong value type
        }
      });
      fail();
    } catch (ClassCastException e) { // expected
      assertThat(e.getMessage(), startsWith("Invalid value type"));
    }
  }

  @Test
  public void testBulkComputeFunctionGetsValuesOfEntries() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    store.bulkCompute(Arrays.asList(1, 2, 3, 4, 5, 6), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        Map<Number, CharSequence> map = makeMap(entries);
        assertThat(map.size(), is(6));
        assertThat(map.get(1), Matchers.<CharSequence>equalTo("one"));
        assertThat(map.get(2), Matchers.<CharSequence>equalTo("two"));
        assertThat(map.get(3), Matchers.<CharSequence>equalTo("three"));
        assertThat(map.get(4), is(nullValue()));
        assertThat(map.get(5), is(nullValue()));
        assertThat(map.get(6), is(nullValue()));

        return null;
      }
    });
  }

  @Test
  public void testBulkComputeHappyPath() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(Arrays.asList(1, 2), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        return new HashMap<Number, CharSequence>() {{
          put(1, "un");
          put(2, "deux");
        }}.entrySet();
      }
    });

    assertThat(result.size(), is(2));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("un"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("deux"));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("un"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("deux"));
  }

  @Test
  public void testBulkComputeIgnoreFunctionReturnedUnspecifiedKeys() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(Arrays.asList(4, 5, 6), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        return new HashMap<Number, CharSequence>() {{
          put(1, "one");
          put(2, "two");
          put(3, "three");
        }}.entrySet();
      }
    });

    assertThat(result, equalTo(Collections.EMPTY_MAP));

    assertThat(store.get(1), is(nullValue()));
    assertThat(store.get(2), is(nullValue()));
    assertThat(store.get(3), is(nullValue()));
  }

  @Test
  public void testBulkComputeDoNothingOnNullFunctionReturn() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(Arrays.asList(2, 1, 5), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        return null;
      }
    });

    assertThat(result, equalTo(Collections.EMPTY_MAP));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(store.get(5), is(nullValue()));
  }

  @Test
  public void testBulkComputeIgnoreEntriesMissingFromFunctionReturn() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(Arrays.asList(1, 2, 3), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        return new HashMap<Number, CharSequence>() {{
          put(2, "deux");
          put(3, "trois");
        }}.entrySet();
      }
    });

    assertThat(result.size(), is(2));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("deux"));
    assertThat(result.get(3).value(), Matchers.<CharSequence>equalTo("trois"));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("deux"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("trois"));
  }

  @Test
  public void testBulkComputeRemoveNullValueEntriesFromFunctionReturn() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkCompute(Arrays.asList(1, 2, 3), new Function<Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> entries) {
        return new HashMap<Number, CharSequence>() {{
          put(1, null);
          put(3, null);
        }}.entrySet();
      }
    });

    assertThat(result.size(), is(2));
    assertThat(result.get(1), is(nullValue()));
    assertThat(result.get(3), is(nullValue()));

    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two"));
  }

  @Test
  public void testBulkComputeIfAbsentFunctionDoesNotGetPresentKeys() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    store.bulkComputeIfAbsent(Arrays.asList(1, 2, 3, 4, 5, 6), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> keys) {
        Set<Number> set = makeSet(keys);
        assertThat(set.size(), is(3));
        assertThat(set, hasItem(4));
        assertThat(set, hasItem(5));
        assertThat(set, hasItem(6));

        return null;
      }
    });
  }

  @Test
  public void testBulkComputeIfAbsentDoesNotOverridePresentKeys() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(Arrays.asList(1, 2, 3, 4, 5, 6), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> numbers) {
        return new HashMap<Number, CharSequence>() {{
          put(1, "un");
          put(2, "deux");
          put(3, "trois");
          put(4, "quatre");
          put(5, "cinq");
          put(6, "six");
        }}.entrySet();
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
  public void testBulkComputeIfAbsentIgnoreFunctionReturnedUnspecifiedKeys() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(Arrays.asList(4, 5, 6), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> numbers) {
        return new HashMap<Number, CharSequence>() {{
          put(1, "one");
          put(2, "two");
          put(3, "three");
        }}.entrySet();
      }
    });

    assertThat(result, equalTo(Collections.EMPTY_MAP));

    assertThat(store.get(1), is(nullValue()));
    assertThat(store.get(2), is(nullValue()));
    assertThat(store.get(3), is(nullValue()));
  }

  @Test
  public void testBulkComputeIfAbsentDoNothingOnNullFunctionReturn() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    store.put(2, "two");
    store.put(3, "three");

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(Arrays.asList(2, 1, 5), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> numbers) {
        return null;
      }
    });

    assertThat(result.size(), is(2));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one"));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    assertThat(store.get(5), is(nullValue()));
  }

  @Test
  public void testBulkComputeIfAbsentIgnoreEntriesMissingFromFunctionReturn() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);

    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(Arrays.asList(1, 2, 3), new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> numbers) {
        return new HashMap<Number, CharSequence>() {{
          put(2, "deux");
          put(3, "trois");
        }}.entrySet();
      }
    });

    assertThat(result.size(), is(2));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("deux"));
    assertThat(result.get(3).value(), Matchers.<CharSequence>equalTo("trois"));

    assertThat(store.get(1), is(nullValue()));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("deux"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("trois"));
  }
  
  @Test
  public void testBulkComputeIfAbsentPicksUpRacingChangesForMissingKeys() throws Exception {
    Store.Configuration<Number, CharSequence> configuration = mock(Store.Configuration.class);
    when(configuration.getKeyType()).thenReturn(Number.class);
    when(configuration.getValueType()).thenReturn(CharSequence.class);

    final OnHeapStore<Number, CharSequence> store = new OnHeapStore<Number, CharSequence>(configuration);
    store.put(1, "one");
    
    Map<Number, Store.ValueHolder<CharSequence>> result = store.bulkComputeIfAbsent(Arrays.asList(1, 2, 3), 
        new Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>() {
      @Override
      public Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>> apply(Iterable<? extends Number> numbers) {
        Set<? super Integer> missingKeys = makeSet(numbers);
        assertThat(missingKeys.size(), is(2));
        assertThat((Iterable<Integer>)missingKeys, hasItems(2, 3));

        // Concurrent mutation of underlying store
        try {
          store.put(2, "two");
          store.put(3, "three");
        } catch (CacheAccessException e) {
          throw new AssertionError(e);
        }
        
        return new HashMap<Number, CharSequence>() {{
          put(2, "deux");
          put(3, "trois");
        }}.entrySet();
      }
    });

    assertThat(result.size(), is(3));
    assertThat(result.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(result.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(result.get(3).value(), Matchers.<CharSequence>equalTo("three"));

    assertThat(store.get(1).value(), Matchers.<CharSequence>equalTo("one"));
    assertThat(store.get(2).value(), Matchers.<CharSequence>equalTo("two"));
    assertThat(store.get(3).value(), Matchers.<CharSequence>equalTo("three"));
    
  }

  public static <K, V> Map<K, V> makeMap(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
    Map<K, V> result = new HashMap<K, V>();
    for (Map.Entry<? extends K, ? extends V> entry : entries) {
      result.put(entry.getKey(), entry.getValue());
    }
    return result;
  }

  public static <E> Set<E> makeSet(Iterable<? extends E> elements) {
    Set<E> result = new HashSet<E>();
    for (E element : elements) {
      result.add(element);
    }
    return result;
  }
  
  private static Iterable<Map.Entry<Object, Object>> makeEntries(Object key, Object value) {
    return Collections.singletonMap(key, value).entrySet();
  }

}
