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

import org.ehcache.Cache;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Predicates;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class StoreComputeTest<K, V> extends SPIStoreTester<K, V> {

  public StoreComputeTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongReturnValueType() throws Exception {
    final Store kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));

    try {
      kvStore.compute("1", new BiFunction() {
        @Override
        public Object apply(Object key, Object oldValue) {
          return this; // returning wrong value type from function
        }
      });
      fail();
    } catch (ClassCastException e) {
      assertThat(e.getMessage(), startsWith("Invalid value type"));
    }
  }
  
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongKeyType() throws Exception {
    final Store kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));

    try {
      kvStore.compute(this, new BiFunction() { // wrong key type
        @Override
        public Object apply(Object key, Object oldValue) {
          throw new AssertionError();
        }
      });
      fail();
    } catch (ClassCastException e) {
      assertThat(e.getMessage(), startsWith("Invalid key type"));
    }
  }
  
  @SPITest
  public void testHappyPath() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));

    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();
    
    kvStore.compute(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K keyParam, V oldValue) {
        assertThat(oldValue, nullValue());
        assertThat(keyParam, is(key));
        return value;
      }
    });
    assertThat(kvStore.get(key).value(), is(value));
    
    final V value2 = factory.createValue(System.nanoTime());
    assertThat(value2, not(equalTo(value)));
    kvStore.compute(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K keyParam, V oldValue) {
        assertThat(oldValue, is(value));
        assertThat(keyParam, is(key));
        return value2;
      }
    });
    assertThat(kvStore.get(key).value(), is(value2));
    
    kvStore.compute(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K keyParam, V oldValue) {
        assertThat(oldValue, is(value2));
        assertThat(keyParam, is(key));
        return null;
      }
    });
    assertThat(kvStore.get(key), nullValue());
  }
  
  
  @SPITest
  public void testException() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));

    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();
  
    kvStore.put(key, value);
    assertThat(kvStore.get(key).value(), is(value));
    
    final RuntimeException re = new RuntimeException();
    try {
      kvStore.compute(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
          assertThat(oldValue, is(value));
          assertThat(keyParam, is(key));
          throw re;
        }
      });
    } catch (RuntimeException e) {
      assertThat(e, is(re));
    }
    
    assertThat(kvStore.get(key).value(), is(value));
  }
}
