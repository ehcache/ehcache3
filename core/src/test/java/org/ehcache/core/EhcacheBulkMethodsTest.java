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
import org.ehcache.core.Ehcache;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.expiry.Expiry;
import org.ehcache.function.Function;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.core.spi.cache.Store.ValueHolder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * @author Ludovic Orban
 */
@SuppressWarnings({ "unchecked", "serial", "rawtypes" })
public class EhcacheBulkMethodsTest {

  private final CacheConfiguration<Number, CharSequence> cacheConfig = mock(CacheConfiguration.class);
  {
    when(cacheConfig.getExpiry()).thenReturn(mock(Expiry.class));
  }

  @Test
  public void testPutAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    CacheEventDispatcher<Number, CharSequence> cacheEventDispatcher = mock(CacheEventDispatcher.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(cacheConfig, store, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class.getName() + ".EhcacheBulkMethodsTest"));
    ehcache.init();

    ehcache.putAll(new HashMap<Number, CharSequence>() {{
      put(1, "one");
      put(2, "two");
      put(3, "three");
    }});

    verify(store).bulkCompute((Set<? extends Number>) Matchers.argThat(hasItems((Number)1, 2, 3)), any(Function.class));
  }

  @Test
  public void testPutAllWithWriter() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function function = (Function)invocation.getArguments()[1];
        function.apply(Arrays.asList(entry(1, "one"), entry(2, "two"), entry(3, "three")));
        return null;
      }
    });
    CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter = mock(CacheLoaderWriter.class);
    CacheEventDispatcher<Number, CharSequence> notifier = mock(CacheEventDispatcher.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(cacheConfig, store, cacheLoaderWriter, notifier, LoggerFactory.getLogger(Ehcache.class.getName() + ".EhcacheBulkMethodsTest1"));
    ehcache.init();

    ehcache.putAll(new LinkedHashMap<Number, CharSequence>() {{
      put(1, "one");
      put(2, "two");
      put(3, "three");
    }});

    verify(store).bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheLoaderWriter).writeAll(argThat(hasItems(entry(1, "one"), entry(2, "two"), entry(3, "three"))));
  }


  @Test
  public void testGetAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkComputeIfAbsent((Set<? extends Number>)argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function function = (Function)invocation.getArguments()[1];
        function.apply(invocation.getArguments()[0]);

        return new HashMap(){{put(1, null); put(2, null); put(3, valueHolder("three")); }};
      }
    });
    CacheEventDispatcher<Number, CharSequence> cacheEventDispatcher = mock(CacheEventDispatcher.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(cacheConfig, store, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class.getName() + ".EhcacheBulkMethodsTest2"));
    ehcache.init();
    Map<Number, CharSequence> result = ehcache.getAll(new HashSet<Number>(Arrays.asList(1, 2, 3)));

    assertThat(result, hasEntry((Number)1, (CharSequence) null));
    assertThat(result, hasEntry((Number)2, (CharSequence) null));
    assertThat(result, hasEntry((Number)3, (CharSequence)"three"));
    verify(store).bulkComputeIfAbsent((Set<? extends Number>)argThat(hasItems(1, 2, 3)), any(Function.class));
  }

  @Test
  public void testGetAllWithLoader() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);

    when(store.bulkComputeIfAbsent((Set<? extends Number>)argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function function = (Function)invocation.getArguments()[1];
        function.apply(invocation.getArguments()[0]);

        final Map<Number, ValueHolder<CharSequence>>loaderValues = new LinkedHashMap<Number, ValueHolder<CharSequence>>();
        loaderValues.put(1, valueHolder((CharSequence)"one"));
        loaderValues.put(2, valueHolder((CharSequence)"two"));
        loaderValues.put(3, null);
        return loaderValues;
      }
    });

    CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter = mock(CacheLoaderWriter.class);
    CacheEventDispatcher<Number, CharSequence> cacheEventDispatcher = mock(CacheEventDispatcher.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(cacheConfig, store, cacheLoaderWriter, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class.getName() + ".EhcacheBulkMethodsTest3"));
    ehcache.init();
    Map<Number, CharSequence> result = ehcache.getAll(new HashSet<Number>(Arrays.asList(1, 2, 3)));

    assertThat(result, hasEntry((Number)1, (CharSequence) "one"));
    assertThat(result, hasEntry((Number)2, (CharSequence) "two"));
    assertThat(result, hasEntry((Number)3, (CharSequence) null));
    verify(store).bulkComputeIfAbsent((Set<? extends Number>)argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheLoaderWriter).loadAll(argThat(hasItems(1, 2, 3)));
  }

  @Test
  public void testRemoveAll() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    CacheEventDispatcher<Number, CharSequence> cacheEventDispatcher = mock(CacheEventDispatcher.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(cacheConfig, store, cacheEventDispatcher, LoggerFactory.getLogger(Ehcache.class.getName() + ".EhcacheBulkMethodsTest4"));
    ehcache.init();
    ehcache.removeAll(new HashSet<Number>(Arrays.asList(1, 2, 3)));

    verify(store).bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class));
  }

  @Test
  public void testRemoveAllWithWriter() throws Exception {
    Store<Number, CharSequence> store = mock(Store.class);
    when(store.bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class))).thenAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        Function function = (Function)invocation.getArguments()[1];
        function.apply(Arrays.asList(entry(1, "one"), entry(2, "two"), entry(3, "three")));
        return null;
      }
    });
    CacheLoaderWriter<Number, CharSequence> cacheLoaderWriter = mock(CacheLoaderWriter.class);
    CacheEventDispatcher<Number, CharSequence> notifier = mock(CacheEventDispatcher.class);

    Ehcache<Number, CharSequence> ehcache = new Ehcache<Number, CharSequence>(cacheConfig, store, cacheLoaderWriter, notifier, LoggerFactory.getLogger(Ehcache.class.getName() + ".EhcacheBulkMethodsTest5"));
    ehcache.init();
    ehcache.removeAll(new LinkedHashSet<Number>(Arrays.asList(1, 2, 3)));

    verify(store).bulkCompute((Set<? extends Number>) argThat(hasItems(1, 2, 3)), any(Function.class));
    verify(cacheLoaderWriter).deleteAll(argThat(hasItems(1, 2, 3)));
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


  private static <V> ValueHolder<V> valueHolder(final V value) {
    return new ValueHolder<V>() {
      @Override
      public V value() {
        return value;
      }

      @Override
      public long creationTime(TimeUnit unit) {
        throw new AssertionError();
      }

      @Override
      public long expirationTime(TimeUnit unit) {
        return 0;
      }

      @Override
      public boolean isExpired(long expirationTime, TimeUnit unit) {
        return false;
      }

      @Override
      public long lastAccessTime(TimeUnit unit) {
        throw new AssertionError();
      }

      @Override
      public float hitRate(long now, TimeUnit unit) {
        throw new AssertionError();
      }

      @Override
      public long hits() {
        throw new UnsupportedOperationException("Implement me!");
      }

      @Override
      public long getId() {
        throw new UnsupportedOperationException("Implement me!");
      }
    };
  }

}
