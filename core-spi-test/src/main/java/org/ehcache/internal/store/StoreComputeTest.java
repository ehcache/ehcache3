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

import org.ehcache.config.Eviction;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.expiry.Expirations;
import org.ehcache.function.BiFunction;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class StoreComputeTest<K, V> extends SPIStoreTester<K, V> {

  public StoreComputeTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongReturnValueType() throws Exception {
    final Store kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory
        .getValueType(), null, Eviction.all(), null, ClassLoader.getSystemClassLoader(), Expirations.noExpiration()));

    if (factory.getValueType() == Object.class) {
      System.err.println("Warning, store uses Object as value type, cannot verify in this configuration");
      return;
    }
    
    final Object value;
    if (factory.getValueType() == String.class) {
      value = this;
    } else {
      value = "value";
    }

    final K key = factory.getKeyType().newInstance();
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
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongKeyType() throws Exception {
    final Store kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory
        .getValueType(), null, Eviction.all(), null, ClassLoader.getSystemClassLoader(), Expirations.noExpiration()));

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
      kvStore.compute(key, new BiFunction() { // wrong key type
            @Override
            public Object apply(Object key, Object oldValue) {
              throw new AssertionError();
            }
          });
      throw new AssertionError();
    } catch (ClassCastException e) {
      // expected
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void testComputePutsValueInStore() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory
        .getValueType(), null, Eviction.all(), null, ClassLoader.getSystemClassLoader(), Expirations.noExpiration()));

    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();

    try {
      kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          return value;
        }
      });
      assertThat(kvStore.get(key).value(), is(value));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void testOverwriteExitingValue() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory
        .getValueType(), null, Eviction.all(), null, ClassLoader.getSystemClassLoader(), Expirations.noExpiration()));

    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();
    final V value2 = factory.createValue(System.nanoTime());

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
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void testNullReturnRemovesEntry() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory
        .getValueType(), null, Eviction.all(), null, ClassLoader.getSystemClassLoader(), Expirations.noExpiration()));

    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();

    try {
      kvStore.put(key, value);
      kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          return null;
        }
      });
      assertThat(kvStore.get(key), nullValue());
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void testException() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(), factory
        .getValueType(), null, Eviction.all(), null, ClassLoader.getSystemClassLoader(), Expirations.noExpiration()));

    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();

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
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }

    assertThat(kvStore.get(key).value(), is(value));
  }
}
