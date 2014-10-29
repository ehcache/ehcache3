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
import org.ehcache.function.Function;
import org.ehcache.function.Predicates;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class StoreComputeIfAbsentTest<K, V> extends SPIStoreTester<K, V> {

  public StoreComputeIfAbsentTest(StoreFactory<K, V> factory) {
    super(factory);
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @SPITest
  public void testWrongReturnValueType() throws Exception {
    final Store kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));

    K key = factory.getKeyType().newInstance();
    
    try {
      kvStore.computeIfAbsent(key, new Function() {
        @Override
        public Object apply(Object key) {
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
      kvStore.computeIfAbsent(this, new Function() { // wrong key type
        @Override
        public Object apply(Object key) {
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
    
    kvStore.computeIfAbsent(key, new Function<K, V>() {
      @Override
      public V apply(K keyParam) {
        assertThat(keyParam, is(key));
        return value;
      }
    });
    assertThat(kvStore.get(key).value(), is(value));
    
    // call again, not absent this time
    kvStore.computeIfAbsent(key, new Function<K, V>() {
      @Override
      public V apply(K keyParam) {
        throw new AssertionError();
      }
    });
    assertThat(kvStore.get(key).value(), is(value));
    
    // return null, no effect
    final AtomicBoolean called = new AtomicBoolean();
    kvStore.clear();
    kvStore.computeIfAbsent(key, new Function<K, V>() {
      @Override
      public V apply(K keyParam) {
        assertThat(keyParam, is(key));
        called.set(true);
        return null;
      }
    });
    assertThat(called.get(), is(true));
    assertThat(kvStore.get(key), nullValue());
  }
  
  
  @SPITest
  public void testException() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(
        factory.getKeyType(), factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));

    final K key = factory.getKeyType().newInstance();
    
    final RuntimeException re = new RuntimeException();
    try {
      kvStore.computeIfAbsent(key, new Function<K, V>() {
        @Override
        public V apply(K keyParam) {
          assertThat(keyParam, is(key));
          throw re;
        }
      });
    } catch (RuntimeException e) {
      assertThat(e, is(re));
    }
    
    assertThat(kvStore.get(key), nullValue());
  }
}
