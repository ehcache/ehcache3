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

import org.ehcache.ValueSupplier;
import org.ehcache.core.spi.function.NullaryFunction;
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.function.BiFunction;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import org.junit.Assert;

public class StoreComputeTest<K, V> extends SPIStoreTester<K, V> {

  public StoreComputeTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  protected Store<K, V> kvStore;
  protected Store kvStore2;

  @After
  public void tearDown() {
    if (kvStore != null) {
      factory.close(kvStore);
      kvStore = null;
    }
    if (kvStore2 != null) {
      @SuppressWarnings("unchecked")
      Store<K, V> kvStore2 = this.kvStore2;
      factory.close(kvStore2);
      this.kvStore2 = null;
    }
  }

  @SuppressWarnings("unchecked")
  @SPITest
  public void testWrongReturnValueType() throws Exception {
    kvStore = factory.newStore();

    if (factory.getValueType() == Object.class) {
      Assert.fail("Warning, store uses Object as value type, cannot verify in this configuration");
    }

    final Object value;
    if (factory.getValueType() == String.class) {
      value = this;
    } else {
      value = "value";
    }

    final K key = factory.createKey(13);
    try {
      kvStore.compute(key, new BiFunction() {
        @Override
        public Object apply(Object key, Object oldValue) {
          return value; // returning wrong value type from function
        }
      });
      throw new AssertionError();
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SuppressWarnings("unchecked")
  @SPITest
  public void testWrongKeyType() throws Exception {
    kvStore2 = factory.newStore();

    if (factory.getKeyType() == Object.class) {
      System.err.println("Warning, store uses Object as key type, cannot verify in this configuration");
      return;
    }

    final Object key;
    if (factory.getKeyType() == String.class) {
      key = this;
    } else {
      key = "key";
    }

    try {
      kvStore2.compute(key, new BiFunction() { // wrong key type
            @Override
            public Object apply(Object key, Object oldValue) {
              throw new AssertionError();
            }
          });
      throw new AssertionError();
    } catch (ClassCastException e) {
      // expected
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testComputePutsValueInStore() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(14);
    final V value = factory.createValue(153);

    try {
      kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          return value;
        }
      });
      assertThat(kvStore.get(key).value(), is(value));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testOverwriteExitingValue() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(151);
    final V value = factory.createValue(1525);
    final V value2 = factory.createValue(1526);

    assertThat(value2, not(equalTo(value)));

    try {
      kvStore.put(key, value);
      kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          return value2;
        }
      });
      assertThat(kvStore.get(key).value(), is(value2));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testNullReturnRemovesEntry() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(1535603985);
    final V value = factory.createValue(15920835);

    try {
      kvStore.put(key, value);
      kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          return null;
        }
      });
      assertThat(kvStore.get(key), nullValue());
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testException() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(520928098);
    final V value = factory.createValue(15098209865L);

    final RuntimeException re = new RuntimeException();

    try {
      kvStore.put(key, value);
      assertThat(kvStore.get(key).value(), is(value));

      kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          throw re;
        }
      });
    } catch (RuntimeException e) {
      assertThat(e, is(re));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(kvStore.get(key).value(), is(value));
  }

  @SPITest
  public void testComputeExpiresOnAccess() throws Exception {
    TestTimeSource timeSource = new TestTimeSource(10042L);
    kvStore = factory.newStoreWithExpiry(new Expiry<K, V>() {
      @Override
      public Duration getExpiryForCreation(K key, V value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(K key, ValueSupplier<? extends V> value) {
        return Duration.ZERO;
      }

      @Override
      public Duration getExpiryForUpdate(K key, ValueSupplier<? extends V> oldValue, V newValue) {
        return Duration.INFINITE;
      }
    }, timeSource);

    final K key = factory.createKey(1042L);
    final V value = factory.createValue(1340142L);

    try {
      kvStore.put(key, value);

      Store.ValueHolder<V> result = kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          return v;
        }
      }, new NullaryFunction<Boolean>() {
        @Override
        public Boolean apply() {
          return false;
        }
      });
      assertThat(result.value(), is(value));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testComputeExpiresOnUpdate() throws Exception {
    TestTimeSource timeSource = new TestTimeSource(10042L);
    kvStore = factory.newStoreWithExpiry(new Expiry<K, V>() {
      @Override
      public Duration getExpiryForCreation(K key, V value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForAccess(K key, ValueSupplier<? extends V> value) {
        return Duration.INFINITE;
      }

      @Override
      public Duration getExpiryForUpdate(K key, ValueSupplier<? extends V> oldValue, V newValue) {
        return Duration.ZERO;
      }
    }, timeSource);

    final K key = factory.createKey(1042L);
    final V value = factory.createValue(1340142L);
    final V newValue = factory.createValue(134054142L);

    try {
      kvStore.put(key, value);

      Store.ValueHolder<V> result = kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K k, V v) {
          return newValue;
        }
      }, new NullaryFunction<Boolean>() {
        @Override
        public Boolean apply() {
          return false;
        }
      });
      assertThat(result.value(), is(newValue));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }
}
