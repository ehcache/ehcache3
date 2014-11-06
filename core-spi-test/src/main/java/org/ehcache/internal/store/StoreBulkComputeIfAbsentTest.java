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
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.function.Predicates;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.test.SPITest;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Test the {@link org.ehcache.spi.cache.Store#bulkComputeIfAbsent(Iterable, org.ehcache.function.Function)} contract of the
 * {@link org.ehcache.spi.cache.Store Store} interface.
 * <p/>
 *
 * @author Gaurav Mangalick
 */

public class StoreBulkComputeIfAbsentTest<K, V> extends SPIStoreTester<K, V> {

  public StoreBulkComputeIfAbsentTest(final StoreFactory<K, V> factory) {
    super(factory);
  }

  @SPITest
  public void testBulkComputeIfReturnAllPassedEntries() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(),
        factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));
    final K k1 = factory.createKey(1L);
    final V v1 = factory.createValue(1L);
    final K k2 = factory.createKey(2L);
    final V v2 = factory.createValue(2L);
    final K k3 = factory.createKey(3L);
    final V v3 = factory.createValue(3L);
    final V v10 = factory.createValue(10L);
    final V v20 = factory.createValue(20L);
    final V v30 = factory.createValue(30L);
    Map<K, V> map = new HashMap<K, V>() {{
      put(k3, v3);
      put(k2, v2);
      put(k1, v1);
    }};
    kvStore.put(k2, v2);
    kvStore.put(k1, v1);
    try {
      Map<K, Store.ValueHolder<V>> result = kvStore.bulkComputeIfAbsent(map.keySet(), new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> ks) {
          return new HashMap<K, V>() {{
            put(k3, v30);
            put(k2, v2);
            put(k1, v10);
          }}.entrySet();
        }
      });
      assertThat(result.get(k3).value(), is(v30));
      assertThat(result.get(k2).value(), is(v2));
      assertThat(result.get(k1).value(), is(v1));
      assertThat(kvStore.get(k3).value(), is(v30));
      assertThat(kvStore.get(k2).value(), is(v2));
      assertThat(kvStore.get(k1).value(), is(v1));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void testBulkComputeIfAbsentFunctionReturnsNull() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(),
        factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));
    final K k1 = factory.createKey(1L);
    final V v1 = factory.createValue(1L);
    final K k2 = factory.createKey(2L);
    final V v2 = factory.createValue(2L);
    final K k3 = factory.createKey(3L);
    final V v3 = factory.createValue(3L);

    Map<K, V> map = new HashMap<K, V>() {{
      put(k3, v3);
      put(k2, v2);
      put(k1, v1);
    }};
    kvStore.put(k2, v2);
    kvStore.put(k1, v1);
    try {
      Map<K, Store.ValueHolder<V>> result = kvStore.bulkComputeIfAbsent(map.keySet(), new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> ks) {
          return null;
        }
      });
      assertThat(result.get(k2).value(), is(v2));
      assertThat(result.get(k1).value(), is(v1));
      assertThat(kvStore.get(k3), is(nullValue()));
      assertThat(kvStore.get(k2).value(), is(v2));
      assertThat(kvStore.get(k1).value(), is(v1));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void testBulkComputeIfAbsentFunctionReturnsLessEntries() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(),
        factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));
    final K k1 = factory.createKey(1L);
    final V v1 = factory.createValue(1L);
    final K k2 = factory.createKey(2L);
    final V v2 = factory.createValue(2L);
    final K k3 = factory.createKey(3L);
    final V v3 = factory.createValue(3L);
    final V v10 = factory.createValue(10L);
    final V v20 = factory.createValue(20L);
    final V v30 = factory.createValue(30L);
    Map<K, V> map = new HashMap<K, V>() {{
      put(k3, v3);
      put(k2, v2);
      put(k1, v1);
    }};
    try {
      Map<K, Store.ValueHolder<V>> result = kvStore.bulkComputeIfAbsent(map.keySet(), new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> ks) {
          return new HashMap<K, V>() {{
            put(k2, v20);
            put(k1, v10);
          }}.entrySet();
        }
      });
      assertThat(result.get(k2).value(), is(v20));
      assertThat(result.get(k1).value(), is(v10));
      assertThat(kvStore.get(k3), is(nullValue()));
      assertThat(kvStore.get(k2).value(), is(v20));
      assertThat(kvStore.get(k1).value(), is(v10));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }

  @SPITest
  public void testBulkComputeFunctionReturnsNullValues() throws Exception {
    final Store<K, V> kvStore = factory.newStore(new StoreConfigurationImpl<K, V>(factory.getKeyType(),
        factory.getValueType(), null, Predicates.<Cache.Entry<K, V>>all(), null, ClassLoader.getSystemClassLoader()));
    final K k1 = factory.createKey(1L);
    final V v1 = factory.createValue(1L);
    final K k2 = factory.createKey(2L);
    final V v2 = factory.createValue(2L);
    final K k3 = factory.createKey(3L);
    final V v3 = factory.createValue(3L);
    Map<K, V> map = new HashMap<K, V>() {{
      put(k3, v3);
      put(k2, v2);
      put(k1, v1);
    }};
    try {
      Map<K, Store.ValueHolder<V>> result = kvStore.bulkComputeIfAbsent(map.keySet(), new Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>>() {
        @Override
        public Iterable<? extends Map.Entry<? extends K, ? extends V>> apply(Iterable<? extends K> ks) {
          return new HashMap<K, V>() {{
            put(k3, null);
            put(k2, null);
          }}.entrySet();
        }
      });
      assertThat(result.get(k3), is(nullValue()));
      assertThat(result.get(k2), is(nullValue()));
      assertThat(result.get(k1), is(nullValue()));
      assertThat(kvStore.get(k3), is(nullValue()));
      assertThat(kvStore.get(k2), is(nullValue()));
      assertThat(kvStore.get(k1), is(nullValue()));
    } catch (CacheAccessException e) {
      System.err.println("Warning, an exception is thrown due to the SPI test");
      e.printStackTrace();
    }
  }
}
