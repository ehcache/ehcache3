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
import org.ehcache.function.BiFunction;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;


import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class StoreComputeIfPresentTest<K, V> extends SPIStoreTester<K, V> {

  public StoreComputeIfPresentTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  protected Store<K, V> kvStore;
  protected Store kvStore2;

  @After
  public void tearDown() {
    if (kvStore != null) {
//      kvStore.close();
      kvStore = null;
    }
    if (kvStore2 != null) {
//      kvStore2.close();
      kvStore2 = null;
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongReturnValueType() throws Exception {
    kvStore = factory.newStore();

    if (factory.getValueType() == Object.class) {
      System.err.println("Warning, store uses Object as value type, cannot verify in this configuration");
      return;
    }
    
    final Object badValue;
    if (factory.getValueType() == String.class) {
      badValue = this;
    } else {
      badValue = "badValue";
    }

    final K key = factory.createKey(1);
    final V value = factory.createValue(1);
    kvStore.put(key, value);

    try {
      kvStore.computeIfPresent(key, new BiFunction() {
        @Override
        public Object apply(Object key, Object oldValue) {
          return badValue; // returning wrong value type from function
        }
      });
      throw new AssertionError();
    } catch (ClassCastException e) {
      // expected
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongKeyType() throws Exception {
    kvStore2 = factory.newStore();

    if (factory.getKeyType() == Object.class) {
      System.err.println("Warning, store uses Object as key type, cannot verify in this configuration");
      return;
    }

    final K key = factory.createKey(1);
    final V value = factory.createValue(1);
    kvStore2.put(key, value);

    final Object badKey;
    if (factory.getKeyType() == String.class) {
      badKey = this;
    } else {
      badKey = "badKey";
    }
    
    try {
      kvStore2.computeIfPresent(badKey, new BiFunction() { // wrong key type
            @Override
            public Object apply(Object key, Object value) {
              throw new AssertionError();
            }
          });
      throw new AssertionError();
    } catch (ClassCastException e) {
      // expected
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testNullReturnRemovesEntry() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(1);
    final V value = factory.createValue(1);
    kvStore.put(key, value);

    try {
      // null return, removes value
      assertThat(kvStore.get(key).value(), is(value));
      kvStore.computeIfPresent(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          return null;
        }
      });
      assertThat(kvStore.get(key), nullValue());
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testComputePutsValueInStoreWhenKeyIsPresent() throws Exception {
    kvStore = factory.newStore();
    final K key = factory.createKey(1);
    final V value = factory.createValue(1);
    final V value2 = factory.createValue(2);
    kvStore.put(key, value);

    try {
      kvStore.computeIfPresent(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          return value2;
        }
      });
      assertThat(kvStore.get(key).value(), is(value2));
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testFunctionNotCalledWhenAbsent() throws Exception {
    kvStore = factory.newStore();
    final K key = factory.createKey(1L);

    try {
      // call with empty store, no effect
      kvStore.computeIfPresent(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          throw new AssertionError();
        }
      });
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testException() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(1);
    final V value = factory.createValue(1);
    kvStore.put(key, value);

    final RuntimeException re = new RuntimeException();
    try {
      kvStore.computeIfPresent(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          throw re;
        }
      });
    } catch (RuntimeException e) {
      assertThat(e, is(re));
    } catch (CacheAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(kvStore.get(key).value(), is(value));
  }
}
