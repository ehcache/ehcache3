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
import org.ehcache.core.spi.store.StoreAccessException;
import org.ehcache.core.spi.function.Function;
import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.TestTimeSource;
import org.ehcache.spi.test.After;
import org.ehcache.spi.test.LegalSPITesterException;
import org.ehcache.spi.test.SPITest;


import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

public class StoreComputeIfAbsentTest<K, V> extends SPIStoreTester<K, V> {

  public StoreComputeIfAbsentTest(StoreFactory<K, V> factory) {
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
      Store<K, V> kvStore2 = (Store<K, V>) this.kvStore2;
      factory.close(kvStore2);
      this.kvStore2 = null;
    }
  }

  @SPITest
  public void testWrongReturnValueType() throws Exception {
    kvStore = factory.newStore();

    if (factory.getValueType() == Object.class) {
      System.err.println("Warning, store uses Object as value type, cannot verify in this configuration");
      return;
    }

    K key = factory.createKey(1L);

    final Object badValue;
    if (factory.getValueType() == String.class) {
      badValue = this;
    } else {
      badValue = "badValue";
    }

    try {
      kvStore.computeIfAbsent(key, new Function<K, V>() {
        @Override
        @SuppressWarnings("unchecked")
        public V apply(K key) {
          return (V) badValue; // returning wrong value type from function
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
  @SuppressWarnings("unchecked")
  public void testWrongKeyType() throws Exception {
    kvStore2 = factory.newStore();

    if (factory.getKeyType() == Object.class) {
      System.err.println("Warning, store uses Object as key type, cannot verify in this configuration");
      return;
    }

    final Object badKey;
    if (factory.getKeyType() == String.class) {
      badKey = this;
    } else {
      badKey = "badKey";
    }

    try {
      kvStore2.computeIfAbsent(badKey, new Function<Object, Object>() { // wrong key type
            @Override
            public Object apply(Object key) {
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
  public void testComputePutsValueInStoreWhenKeyIsAbsent() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(1);
    final V value = factory.createValue(1);

    assertThat(kvStore.get(key), nullValue());
    try {
      kvStore.computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(K keyParam) {
          return value;
        }
      });
      assertThat(kvStore.get(key).value(), is(value));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }

  @SPITest
  public void testFunctionNotInvokedWhenPresent() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(1);
    final V value = factory.createValue(1);

    kvStore.put(key, value);

    // if entry is not absent, the function should not be invoked
    try {
      kvStore.computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(K keyParam) {
          throw new AssertionError();
        }
      });
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
    assertThat(kvStore.get(key).value(), is(value));
  }

  @SPITest
  public void testFunctionReturnsNull() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(1L);;

    // return null, no effect
    assertThat(kvStore.get(key), nullValue());
    final AtomicBoolean called = new AtomicBoolean();
    try {
      kvStore.computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(K keyParam) {
          called.set(true);
          return null;
        }
      });
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
    assertThat(called.get(), is(true));
    assertThat(kvStore.get(key), nullValue());
  }

  @SPITest
  public void testException() throws Exception {
    kvStore = factory.newStore();

    final K key = factory.createKey(1L);;

    assertThat(kvStore.get(key), nullValue());

    final RuntimeException re = new RuntimeException();
    try {
      kvStore.computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(K keyParam) {
          throw re;
        }
      });
    } catch (RuntimeException e) {
      assertThat(e, is(re));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }

    assertThat(kvStore.get(key), nullValue());
  }

  @SPITest
  public void testComputeIfAbsentValuePresentExpiresOnAccess() throws LegalSPITesterException {
    TestTimeSource timeSource = new TestTimeSource(10043L);
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

    K key = factory.createKey(250928L);
    V value = factory.createValue(2059820L);
    final V newValue = factory.createValue(205982025L);

    try {
      kvStore.put(key, value);
      Store.ValueHolder<V> result = kvStore.computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(K k) {
          fail("Should not be invoked");
          return newValue;
        }
      });
      assertThat(result.value(), is(value));
    } catch (StoreAccessException e) {
      throw new LegalSPITesterException("Warning, an exception is thrown due to the SPI test");
    }
  }
}
