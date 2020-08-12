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

import org.ehcache.core.exceptions.StorePassThroughException;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.Before;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;


import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test the {@link Store#bulkCompute(java.util.Set, Function)} contract of the
 * {@link Store Store} interface.
 *
 * @author Gaurav Mangalick
 */

public class StoreBulkComputeTest<K, V> extends SPIStoreTester<K, V> {

  public StoreBulkComputeTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  private Store<K, V> kvStore;

  @Before
  public void setUp() {
    kvStore = factory.newStore();
  }

  @After
  public void tearDown() {
    if (kvStore != null) {
      factory.close(kvStore);
      kvStore = null;
    }
  }

  @SuppressWarnings({ "unchecked" })
  @SPITest
  public void remappingFunctionReturnsIterableOfEntriesForEachInputEntry() throws Exception {
    Set<K> inputKeys = new HashSet<>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
      K k = factory.createKey(i);
      V v = factory.createValue(i);

      inputKeys.add(k);
      kvStore.put(k, v);
    }

    try {
      Map<K, Store.ValueHolder<V>> mapFromRemappingFunction = kvStore.bulkCompute(inputKeys, entries -> {
        Map<K, V> update = new HashMap<>();
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          update.put(entry.getKey(), entry.getValue());
        }
        return update.entrySet();
      }
      );
      assertThat(mapFromRemappingFunction.keySet(), containsInAnyOrder((K[])inputKeys.toArray()));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SuppressWarnings({ "unchecked" })
  @SPITest
  public void testWrongKeyType() throws Exception {
    Set<K> inputKeys = new HashSet<>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {

      if (factory.getKeyType() == String.class) {
        inputKeys.add((K)new Object());
      } else {
        inputKeys.add((K)"WrongKeyType");
      }
    }

    try {
      kvStore.bulkCompute(inputKeys, entries -> {
        throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
      });
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void mappingIsRemovedFromStoreForNullValueEntriesFromRemappingFunction() throws Exception {
    Set<K> inputKeys = new HashSet<>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
      K k = factory.createKey(i);
      V v = factory.createValue(i);

      inputKeys.add(k);
      kvStore.put(k, v);
    }

    try {
      kvStore.bulkCompute(inputKeys, entries -> {
        Map<K, V> update = new HashMap<>();
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          update.put(entry.getKey(), null);
        }
        return update.entrySet();
      });
      for (K key : inputKeys) {
        assertThat(kvStore.get(key), is(nullValue()));
      }
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void remappingFunctionGetsIterableWithMappedStoreEntryValueOrNull() throws Exception {
    Set<K> inputKeys = new HashSet<>();
    final Map<K, V> mappedEntries = new HashMap<>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
      K k = factory.createKey(i);
      V v = factory.createValue(i);

      inputKeys.add(k);

      if (i % 2 == 0) { // Half of the keys will have mapped store entries, others will have no mapping (null)
        kvStore.put(k, v);
        mappedEntries.put(k, v);
      }
    }

    try {
      kvStore.bulkCompute(inputKeys, entries -> {
        Map<K, V> update = new HashMap<>();
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          if (mappedEntries.containsKey(entry.getKey())) {
            assertThat(entry.getValue(), is(mappedEntries.get(entry.getKey())));
          } else {
            assertThat(entry.getValue(), is(nullValue()));
          }
          update.put(entry.getKey(), entry.getValue());
        }
        return update.entrySet();
      }
      );
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void computeValuesForEveryKeyUsingARemappingFunction() throws Exception {
    Set<K> inputKeys = new HashSet<>();
    final Map<K, V> computedEntries = new HashMap<>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
      K k = factory.createKey(i);
      V v = factory.createValue(i);

      inputKeys.add(k);
      computedEntries.put(k, v);
    }

    try {
      kvStore.bulkCompute(inputKeys, entries -> {
        Map<K, V> update = new HashMap<>();
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          update.put(entry.getKey(), computedEntries.get(entry.getKey()));
        }
        return update.entrySet();
      });

      for (K inputKey : inputKeys) {
        assertThat(kvStore.get(inputKey).get(), is(computedEntries.get(inputKey)));
      }
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SuppressWarnings({ "unchecked" })
  @SPITest
  public void remappingFunctionProducesWrongKeyType() throws Exception {
    Set<K> inputKeys = new HashSet<>();
    final Map<K, V> computedEntries = new HashMap<>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
      K k = factory.createKey(i);
      V v = factory.createValue(i);

      inputKeys.add(k);
      computedEntries.put(k, v);
    }

    try {
      kvStore.bulkCompute(inputKeys, entries -> {
        Map<K, V> update = new HashMap<>();
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          if (factory.getKeyType() == String.class) {
            update.put((K)new StringBuffer(entry.getKey().toString()), computedEntries.get(entry.getKey()));
          } else {
            update.put((K)entry.getKey().toString(), computedEntries.get(entry.getKey()));
          }
        }
        return update.entrySet();
      });
      throw new AssertionError("Expected ClassCastException because the key is of the wrong type");
    } catch (ClassCastException cce) {
      //expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SuppressWarnings({ "unchecked" })
  @SPITest
  public void remappingFunctionProducesWrongValueType() throws Exception {
    Set<K> inputKeys = new HashSet<>();
    final Map<K, V> computedEntries = new HashMap<>();
    int nbElements = 10;
    for (long i = 0; i < nbElements; i++) {
      K k = factory.createKey(i);
      V v = factory.createValue(i);

      inputKeys.add(k);
      computedEntries.put(k, v);
    }

    try {
      kvStore.bulkCompute(inputKeys, entries -> {
        Map<K, V> update = new HashMap<>();
        for (Map.Entry<? extends K, ? extends V> entry : entries) {
          if (factory.getKeyType() == String.class) {
            update.put(entry.getKey(), (V)new StringBuffer(computedEntries.get(entry.getKey()).toString()));
          } else {
            update.put(entry.getKey(), (V)computedEntries.get(entry.getKey()).toString());
          }
        }
        return update.entrySet();
      });
      throw new AssertionError("Expected ClassCastException because the value is of the wrong type");
    } catch (ClassCastException cce) {
      //expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void exception() throws Exception {
    Set<K> inputKeys = Collections.singleton(factory.createKey(0));

    RuntimeException exception = new RuntimeException("error");

    try {
      kvStore.bulkCompute(inputKeys, entries -> { throw exception; });
    } catch (StoreAccessException e) {
      assertThat(e.getCause(), is(exception));
    }
  }

  @SPITest
  public void passThroughException() throws Exception {
    Set<K> inputKeys = Collections.singleton(factory.createKey(0));

    RuntimeException exception = new RuntimeException("error");
    StorePassThroughException ste = new StorePassThroughException(exception);

    try {
      kvStore.bulkCompute(inputKeys, entries -> { throw ste; });
    } catch (RuntimeException e) {
      assertThat(e, is(exception));
    }
  }
}
