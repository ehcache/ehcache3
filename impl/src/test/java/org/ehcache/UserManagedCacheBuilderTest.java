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

import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.UserManagedCacheConfiguration;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.spi.ServiceLocator;
import org.junit.Test;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertNotNull;

public class UserManagedCacheBuilderTest {

  @Test
  public void testIsExtensible() {

    final UserManagedCacheConfiguration<String, Object, TestUserManagedCache<String, Object>> cfg = new UserManagedCacheConfiguration<String, Object, TestUserManagedCache<String, Object>>() {
      @Override
      public UserManagedCacheBuilder<String, Object, TestUserManagedCache<String, Object>> builder(final UserManagedCacheBuilder<String, Object, ? extends UserManagedCache<String, Object>> builder) {
        return new UserManagedCacheBuilder<String, Object, TestUserManagedCache<String, Object>>(String.class, Object.class) {
          @Override
          TestUserManagedCache<String, Object> build(final ServiceLocator serviceProvider) {
            return new TestUserManagedCache<String, Object>();
          }
        };
      }
    };
    
    assertNotNull(cfg);

    // OpenJDK 1.6's javac is not happy about the type inference here...
    // java version "1.6.0_32"
    // OpenJDK Runtime Environment (IcedTea6 1.13.4) (6b32-1.13.4-4ubuntu0.12.04.2)
    // OpenJDK 64-Bit Server VM (build 23.25-b01, mixed mode)
    // javac 1.6.0_32

//    final TestUserManagedCache<String, Object> cache = newCacheBuilder(String.class, Object.class)
//        .with(cfg).build();
//    assertThat(cache, notNullValue());
//    assertThat(cache, is(instanceOf(TestUserManagedCache.class)));
  }

  private class TestUserManagedCache<K, V> implements PersistentUserManagedCache<K, V> {
    @Override
    public Maintainable toMaintenance() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void init() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void close() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Status getStatus() {
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
    public void remove(final K key) {
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

    @Override
    public V putIfAbsent(K key, V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean remove(K key, V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public V replace(K key, V value) throws NullPointerException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public boolean replace(K key, V oldValue, V newValue) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public Map<K, V> getAll(Set<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void removeAll(Set<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

  }
}
