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

import org.ehcache.function.Function;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.writer.CacheWriter;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
public class EhcacheBulkMethodsTest {

  @Test
  public void testPutAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store);

    ehcache.putAll(new HashMap<Number, CharSequence>() {{
      put(1, "one");
      put(2, "two");
      put(3, "three");
    }}.entrySet());

    verify(store).bulkCompute(argThat(hasItems(1, 2, 3)), any(Function.class));
  }

  @Test
  public void testPutAllWithWriter() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkCompute(argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function function = (Function)invocation.getArguments()[1];
        function.apply(Arrays.asList(entry(2, "two"), entry(1, "one"), entry(3, "three")));
        return null;
      }
    });
    CacheWriter<Number, CharSequence> cacheWriter = mock(CacheWriter.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store, null, cacheWriter);

    ehcache.putAll(new HashMap<Number, CharSequence>() {{
      put(3, "three");
      put(2, "two");
      put(1, "one");
    }}.entrySet());

    verify(store).bulkCompute(argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheWriter).writeAll(argThat(hasItems(entry(1, "one"), entry(2, "two"), entry(3, "three"))));
  }

  @Test
  public void testGetAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store);
    Map<Number, CharSequence> result = ehcache.getAll(Arrays.asList(1, 2, 3));

    assertThat(result, equalTo(Collections.<Number, CharSequence>emptyMap()));
    verify(store).bulkComputeIfAbsent(argThat(hasItems(1, 2, 3)), any(Function.class));
  }

  @Test
  public void testGetAllWithLoader() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkComputeIfAbsent(argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function function = (Function)invocation.getArguments()[1];
        function.apply(invocation.getArguments()[0]);
        return null;
      }
    });
    CacheLoader<Number, CharSequence> cacheLoader = mock(CacheLoader.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store, cacheLoader);
    Map<Number, CharSequence> result = ehcache.getAll(Arrays.asList(1, 2, 3));

    assertThat(result, equalTo(Collections.<Number, CharSequence>emptyMap()));
    verify(store).bulkComputeIfAbsent(argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheLoader).loadAll(argThat(hasItems(1, 2, 3)));
  }

  @Test
  public void testRemoveAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store);
    ehcache.removeAll(Arrays.asList(1, 2, 3));

    verify(store).bulkCompute(argThat(hasItems(1, 2, 3)), any(Function.class));
  }

  @Test
  public void testRemoveAllWithWriter() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkCompute(argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function function = (Function)invocation.getArguments()[1];
        function.apply(Arrays.asList(entry(2, "two"), entry(1, "one"), entry(3, "three")));
        return null;
      }
    });
    CacheWriter<Number, CharSequence> cacheWriter = mock(CacheWriter.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(store, null, cacheWriter);
    ehcache.removeAll(Arrays.asList(1, 2, 3));

    verify(store).bulkCompute(argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheWriter).deleteAll(argThat(hasItems(1, 2, 3)));
  }


  private static <K, V> Map.Entry<? extends K, ? extends V> entry(final K key, final V value) {
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

}
