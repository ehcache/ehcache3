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

package org.ehcache.core;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * @author Ludovic Orban
 */
@SuppressWarnings("unchecked")
public class EhcacheBulkMethodsTest {

  @Test
  public void testPutAll() throws Exception {
    @SuppressWarnings("unchecked")
    Store<Number, CharSequence> store = mock(Store.class);

    InternalCache<Number, CharSequence> ehcache = getCache(store);
    ehcache.init();

    Map<Number, CharSequence> map = new HashMap<>(3);
    map.put(1, "one");
    map.put(2, "two");
    map.put(3, "three");
    ehcache.putAll(map);

    verify(store).bulkCompute((Set<? extends Number>) argThat(hasItems((Number)1, 2, 3)), any(Function.class));
  }

  @Test
  public void testGetAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkComputeIfAbsent((Set<? extends Number>)argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(invocation -> {
      Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>> function =
        (Function<Iterable<? extends Number>, Iterable<? extends Map.Entry<? extends Number, ? extends CharSequence>>>) invocation.getArguments()[1];
      Set<? extends Number> keys = (Set<? extends Number>) invocation.getArguments()[0];

      function.apply(keys);

      Map<Number, ValueHolder<String>> map =  new HashMap<>();
      map.put(1, null);
      map.put(2, null);
      map.put(3, valueHolder("three"));
      return map;
    });

    InternalCache<Number, CharSequence> ehcache = getCache(store);
    ehcache.init();
    Map<Number, CharSequence> result = ehcache.getAll(new HashSet<Number>(Arrays.asList(1, 2, 3)));

    assertThat(result, hasEntry(1, null));
    assertThat(result, hasEntry(2, null));
    assertThat(result, hasEntry(3, "three"));
    verify(store).bulkComputeIfAbsent((Set<? extends Number>)argThat(hasItems(1, 2, 3)), any(Function.class));
  }

  @Test
  public void testRemoveAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    InternalCache<Number, CharSequence> ehcache = getCache(store);
    ehcache.init();
    ehcache.removeAll(new HashSet<Number>(Arrays.asList(1, 2, 3)));

    verify(store).bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class));
  }

  protected InternalCache<Number, CharSequence> getCache(Store<Number, CharSequence> store) {
    CacheConfiguration<Number, CharSequence> cacheConfig = mock(CacheConfiguration.class);
    when(cacheConfig.getExpiryPolicy()).thenReturn(mock(ExpiryPolicy.class));
    CacheEventDispatcher<Number, CharSequence> cacheEventDispatcher = mock(CacheEventDispatcher.class);
    ResilienceStrategy<Number, CharSequence> resilienceStrategy = mock(ResilienceStrategy.class);
    return new Ehcache<>(cacheConfig, store, resilienceStrategy, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class + "-" + "EhcacheBulkMethodsTest"));
  }

  static <K, V> Map.Entry<K, V> entry(final K key, final V value) {
    return new Map.Entry<K, V>() {

      @Override
      public K getKey() {
        return key;
      }

      @Override
      public V getValue() {
        return value;
      }

      @Override
      public V setValue(V value) {
        return null;
      }

      @Override
      public int hashCode() {
        return key.hashCode() + value.hashCode();
      }

      @Override
      public boolean equals(Object obj) {
        if (obj.getClass() == getClass()) {
          Map.Entry<K, V> other = (Map.Entry<K, V>)obj;
          return key.equals(other.getKey()) && value.equals(other.getValue());
        }
        return false;
      }

      @Override
      public String toString() {
        return key + "/" + value;
      }
    };
  }


  static <V> ValueHolder<V> valueHolder(final V value) {
    return new ValueHolder<V>() {
      @Override
      public V get() {
        return value;
      }

      @Override
      public long creationTime() {
        throw new AssertionError();
      }

      @Override
      public long expirationTime() {
        return 0;
      }

      @Override
      public boolean isExpired(long expirationTime) {
        return false;
      }

      @Override
      public long lastAccessTime() {
        throw new AssertionError();
      }

      @Override
      public long getId() {
        throw new UnsupportedOperationException("Implement me!");
      }
    };
  }

}
