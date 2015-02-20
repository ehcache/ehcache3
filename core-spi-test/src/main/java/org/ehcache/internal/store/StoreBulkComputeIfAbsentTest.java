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
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.SPITest;


import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * Test the {@link org.ehcache.spi.cache.Store#bulkComputeIfAbsent(java.util.Set, org.ehcache.function.Function)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Gaurav Mangalick
 */

public class StoreBulkComputeIfAbsentTest<K, V> extends SPIStoreTester<K, V> {

  public StoreBulkComputeIfAbsentTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  protected Store<K, V> kvStore;

  @After
  public void tearDown() {
    if (kvStore != null) {
      kvStore.close();
      kvStore = null;
    }
  }

  @SPITest
  public void mappingFunctionReturnsIterableOfEntriesForEachInputEntry() throws Exception {
    kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    Set<K> inputKeys = new HashSet<K>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
       K k = factory.createKey(i);
       V v = factory.createValue(i);

      inputKeys.add(k);
      kvStore.put(k, v);
    }

    try {
      kvStore.bulkComputeIfAbsent(inputKeys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
            @Override
            public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> entries) {
              assertThat(entries.iterator().hasNext(), is(false));
              return new HashMap<K, V>().entrySet();
            }
          }
      );
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void missingIterableEntriesAreIgnoredByTheStore() throws Exception {
    kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    Set<K> inputKeys = new HashSet<K>();
    Map<K, V> mappedEntries = new HashMap<K, V>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
       K k = factory.createKey(i);
       V v = factory.createValue(i);

      inputKeys.add(k);
      mappedEntries.put(k, v);
      kvStore.put(k, v);
    }

    try {
      kvStore.bulkComputeIfAbsent(inputKeys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
            @Override
            public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> entries) {
              return new HashMap<K, V>().entrySet();
            }
          }
      );

      for (Map.Entry<K, V> mappedEntry : mappedEntries.entrySet()) {
        assertThat(kvStore.get(mappedEntry.getKey()).value(), is(mappedEntry.getValue()));
      }
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void mappingIsSameInTheStoreForEntriesReturnedWithDifferentValueFromMappingFunction() throws Exception {
    kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    Set<K> inputKeys = new HashSet<K>();
    Map<K, V> mappedEntries = new HashMap<K, V>();
    final Map<K, V> computedEntries = new HashMap<K, V>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
      K k = factory.createKey(i);
      V v = factory.createValue(i);
      V updatedV = factory.createValue(i * 2);

      inputKeys.add(k);
      mappedEntries.put(k, v);
      computedEntries.put(k, updatedV);
      kvStore.put(k, v);
    }

    try {
      kvStore.bulkComputeIfAbsent(inputKeys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> entries) {
          Map<K, V> update = new HashMap<K, V>();
          for (K key : entries) {
            update.put(key, computedEntries.get(key));
          }
          return update.entrySet();
        }
      });

      for (Map.Entry<K, V> mappedEntry : mappedEntries.entrySet()) {
        assertThat(kvStore.get(mappedEntry.getKey()).value(), is(mappedEntry.getValue()));
      }
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "unchecked" })
  @SPITest
  public void testWrongKeyType() throws Exception {
    kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

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
      kvStore.bulkComputeIfAbsent(inputKeys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> entries) {
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

  @SPITest
  public void computeValuesForEveryKeyUsingAMappingFunction() throws Exception {
    kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    Set<K> inputKeys = new HashSet<K>();
    final Map<K, V> computedEntries = new HashMap<K, V>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
       K k = factory.createKey(i);
       V v = factory.createValue(i);

      inputKeys.add(k);
      computedEntries.put(k, v);
    }
    try {
      kvStore.bulkComputeIfAbsent(inputKeys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> entries) {
          Map<K, V> update = new HashMap<K, V>();
          for (K key : entries) {
            update.put(key, computedEntries.get(key));
          }
          return update.entrySet();
        }
      });

      for (Map.Entry<K, V> entry : computedEntries.entrySet()) {
        assertThat(kvStore.get(entry.getKey()).value(), is(entry.getValue()));
      }
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "unchecked" })
  @SPITest
  public void testMappingFunctionProducesWrongKeyType() throws Exception {
    kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    Set<K> inputKeys = new HashSet<K>();
    final Map<K, V> computedEntries = new HashMap<K, V>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
       K k = factory.createKey(i);
       V v = factory.createValue(i);

      inputKeys.add(k);
      computedEntries.put(k, v);
    }

    try {
      kvStore.bulkComputeIfAbsent(inputKeys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> entries) {
          Map<K, V> update = new HashMap<K, V>();
          for (K key : entries) {
            if (factory.getKeyType() == String.class) {
              update.put((K)new StringBuffer(key.toString()), computedEntries.get(key));
            } else {
              update.put((K)key.toString(), computedEntries.get(key));
            }
          }
          return update.entrySet();
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

  @SuppressWarnings({ "unchecked" })
  @SPITest
  public void testMappingFunctionProducesWrongValueType() throws Exception {
    kvStore = factory.newStore(factory.newConfiguration(factory.getKeyType(), factory.getValueType(), null, null, null));

    Set<K> inputKeys = new HashSet<K>();
    final Map<K, V> computedEntries = new HashMap<K, V>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
       K k = factory.createKey(i);
       V v = factory.createValue(i);

      inputKeys.add(k);
      computedEntries.put(k, v);
    }

    try {
      kvStore.bulkComputeIfAbsent(inputKeys, new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> entries) {
          Map<K, V> update = new HashMap<K, V>();
          for (K key : entries) {
            if (factory.getKeyType() == String.class) {
              update.put(key, (V)new StringBuffer(computedEntries.get(key).toString()));
            } else {
              update.put(key, (V)computedEntries.get(key).toString());
            }
          }
          return update.entrySet();
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
