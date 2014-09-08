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

package org.ehcache;

import org.ehcache.config.StandaloneCacheConfiguration;
import org.ehcache.spi.ServiceLocator;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

public class StandaloneCacheBuilderTest {

  @Test
  public void testIsExtensible() {

    final StandaloneCacheConfiguration<String, Object, TestStandaloneCache<String, Object>> cfg = new StandaloneCacheConfiguration<String, Object, TestStandaloneCache<String, Object>>() {
      @Override
      public StandaloneCacheBuilder<String, Object, TestStandaloneCache<String, Object>> builder(final StandaloneCacheBuilder<String, Object, ? extends StandaloneCache<String, Object>> builder) {
        return new StandaloneCacheBuilder<String, Object, TestStandaloneCache<String, Object>>(String.class, Object.class) {
          @Override
          TestStandaloneCache<String, Object> build(final ServiceLocator serviceProvider) {
            return new TestStandaloneCache<String, Object>();
          }
        };
      }
    };

    // OpenJDK 1.6's javac is not happy about the type inference here...
    // java version "1.6.0_32"
    // OpenJDK Runtime Environment (IcedTea6 1.13.4) (6b32-1.13.4-4ubuntu0.12.04.2)
    // OpenJDK 64-Bit Server VM (build 23.25-b01, mixed mode)
    // javac 1.6.0_32

//    final TestStandaloneCache<String, Object> cache = newCacheBuilder(String.class, Object.class)
//        .with(cfg).build();
//    assertThat(cache, notNullValue());
//    assertThat(cache, is(instanceOf(TestStandaloneCache.class)));
  }

  private class TestStandaloneCache<K, V> implements PersistentStandaloneCache<K, V> {
    @Override
    public void destroy() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V get(final K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void put(final K key, final V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean containsKey(final K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean remove(final K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V getAndRemove(final K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V getAndPut(final K key, final V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Map<K, V> getAll(final Set<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void removeAll(final Set<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void putAll(final Map<? extends K, ? extends V> map) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean putIfAbsent(final K key, final V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean remove(final K key, final V oldValue) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean replace(final K key, final V oldValue, final V newValue) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean replace(final K key, final V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V getAndReplace(final K key, final V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void removeAll() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void clear() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Iterator<Entry<K, V>> iterator() {
      throw new UnsupportedOperationException("Implement me!");
    }
  }
}