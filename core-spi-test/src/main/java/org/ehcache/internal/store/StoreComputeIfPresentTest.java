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

import java.util.concurrent.atomic.AtomicBoolean;

import org.ehcache.Cache;
import org.ehcache.config.StoreConfigurationImpl;
import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.Predicates;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class StoreComputeIfPresentTest<K, V> extends SPIStoreTester<K, V> {

  public StoreComputeIfPresentTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongReturnValueType() throws Exception {
    final Store kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));

    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();
    kvStore.put(key, value);
    
    try {
      kvStore.computeIfPresent(key, new BiFunction() {
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

    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();
    kvStore.put(key, value);
    
    try {
      kvStore.computeIfPresent(this, new BiFunction() { // wrong key type
        @Override
        public Object apply(Object key, Object value) {
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
    kvStore.put(key, value);
    
    // null return, removes value
    assertThat(kvStore.get(key).value(), is(value));
    kvStore.computeIfPresent(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K keyParam, V oldValue) {
        assertThat(keyParam, is(key));
        assertThat(oldValue, is(value));
        return null;
      }
    });
    assertThat(kvStore.get(key), nullValue());
    
    // call again, this time returning new value
    final V value2 = factory.createValue(System.nanoTime());
    kvStore.put(key, value);
    kvStore.computeIfPresent(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K keyParam, V oldValue) {
        assertThat(keyParam, is(key));
        assertThat(oldValue, is(value));
        return value2;
      }
    });
    assertThat(kvStore.get(key).value(), is(value2));
    
    // call with empty store, no effect
    kvStore.clear();
    kvStore.computeIfPresent(key, new BiFunction<K, V, V>() {
      @Override
      public V apply(K keyParam, V oldValue) {
        throw new AssertionError();
      }
    });
  }
  
  
  @SPITest
  public void testException() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));
    
    final K key = factory.getKeyType().newInstance();
    final V value = factory.getValueType().newInstance();
    kvStore.put(key, value);
    
    final RuntimeException re = new RuntimeException();
    try {
      kvStore.computeIfPresent(key, new BiFunction<K, V, V>() {
        @Override
        public V apply(K keyParam, V oldValue) {
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
