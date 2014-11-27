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
import org.ehcache.spi.test.SPITest;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test the {@link org.ehcache.spi.cache.Store#bulkCompute(Iterable, org.ehcache.function.Function)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Gaurav Mangalick
 */

public class StoreBulkComputeTest<K, V> extends SPIStoreTester<K, V> {

  public StoreBulkComputeTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SuppressWarnings({ "unchecked" })
  @SPITest
  public void remappingFunctionReturnsIterableOfEntriesForEachInputEntry() throws Exception {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    Set<K> inputKeys = new HashSet<K>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
      final K k = factory.createKey(i);
      final V v = factory.createValue(i);

      inputKeys.add(k);
      kvStore.put(k, v);
    }

    try {
      Map<K, Store.ValueHolder<V>> mapFromRemappingFunction = kvStore.bulkCompute(inputKeys, new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
            @Override
            public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
              Map<K, V> map = new HashMap<K, V>();
              for (final Map.Entry<? extends K, ? extends V> entry : entries) {
                map.put(entry.getKey(), entry.getValue());
              }
              return map.entrySet();
            }
          }
      );
      assertThat(mapFromRemappingFunction.keySet(), containsInAnyOrder((K[])inputKeys.toArray()));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongKeyType() throws Exception {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    Set<K> inputKeys = new HashSet<K>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {

      if (factory.getKeyType() == String.class) {
        inputKeys.add((K)new Object());
      } else {
        inputKeys.add((K)"WrongKeyType");
      }
    }

    try {
      kvStore.bulkCompute(inputKeys, new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
        }
      });
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void mappingIsRemovedFromStoreForNullValueEntriesFromRemappingFunction() throws Exception {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));
    final K k1 = factory.createKey(1L);
    final V v1 = factory.createValue(1L);

    Set<K> set = new HashSet<K>();
    set.add(k1);

    kvStore.put(k1, v1);

    try {
      kvStore.bulkCompute(Arrays.asList((K[])set.toArray()), new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          Map<K, V> map = new HashMap<K, V>();
          map.put(k1, null);
          return map.entrySet();
        }
      });
      assertThat(kvStore.get(k1), is(nullValue()));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void remappingFunctionGetsIterableWithMappedStoreEntryValueOrNull() throws Exception {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));
    final K k1 = factory.createKey(1L);

    Set<K> set = new HashSet<K>();
    set.add(k1);

    try {
      kvStore.bulkCompute(Arrays.asList((K[])set.toArray()), new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
            @Override
            public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
              Map.Entry<? extends K, ? extends V> entry = entries.iterator().next();
              assertThat(entry.getValue(), is(nullValue()));
              Map<K, V> map = new HashMap<K, V>();
              map.put(entry.getKey(), entry.getValue());
              return map.entrySet();
            }
          }
      );
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void computeValuesForEveryKeyUsingARemappingFunction() throws Exception {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));
    final K k1 = factory.createKey(1L);
    final V v1 = factory.createValue(1L);

    Set<K> set = new HashSet<K>();
    set.add(k1);

    try {
      kvStore.bulkCompute(Arrays.asList((K[])set.toArray()), new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          Map<K, V> map = new HashMap<K, V>();
          map.put(k1, v1);
          return map.entrySet();
        }
      });
      assertThat(kvStore.get(k1).value(), is(v1));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testRemappingFunctionProducesWrongKeyType() throws Exception {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));
    final K k1 = factory.createKey(1L);
    final V v1 = factory.createValue(1L);
    Set<K> set = new HashSet<K>();
    set.add(k1);
    try {
      kvStore.bulkCompute(Arrays.asList((K[])set.toArray()), new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          Map<K, V> map = new HashMap<K, V>();
          if (factory.getKeyType() == String.class) {
            map.put((K)new Object(), v1);
          } else {
            map.put((K)"WrongKeyType", v1);
          }
          return map.entrySet();
        }
      });
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException cce) {
      //expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testRemappingFunctionProducesWrongValueType() throws Exception {
    final Store<K, V> kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));
    final K k1 = factory.createKey(1L);
    Set<K> set = new HashSet<K>();
    set.add(k1);
    try {
      kvStore.bulkCompute(Arrays.asList((K[])set.toArray()), new Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
          Map<K, V> map = new HashMap<K, V>();
          if (factory.getValueType() == String.class) {
            map.put(k1, (V)new Object());
          } else {
            map.put(k1, (V)"WrongValueType");
          }
          return map.entrySet();
        }
      });
      throw new AssertionError("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException cce) {
      //expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }
}
