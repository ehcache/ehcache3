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
package org.ehcache.integration;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.resilience.StoreAccessException;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.core.events.NullStoreEventDispatcher;
import org.ehcache.impl.internal.sizeof.NoopSizeOfEngine;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.core.spi.time.SystemTimeSource;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.impl.internal.spi.serialization.DefaultSerializationProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.ServiceLocator.dependencySet;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Tests for bulk processing methods in {@link org.ehcache.core.Ehcache Ehcache}.
 */
@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
public class EhcacheBulkMethodsITest {

  @Test
  public void testPutAll_without_cache_writer() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.build();

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    myCache.putAll(stringStringHashMap);

    for (int i = 0; i < 3; i++) {
      assertThat(myCache.get("key" + i), is("value" + i));
    }

  }

  @Test
  public void testPutAll_with_cache_writer() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriter = mock(CacheLoaderWriter.class);
    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheLoaderWriter).write(ArgumentMatchers
        .any(), ArgumentMatchers.any());
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriter);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider);
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.withLoaderWriter(cacheLoaderWriter)
            .build();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    myCache.putAll(stringStringHashMap);

    verify(cacheLoaderWriter, times(3)).writeAll(ArgumentMatchers.any(Iterable.class));
    Set set = new HashSet(){{add(entry("key0", "value0"));}};
    verify(cacheLoaderWriter).writeAll(set);
    set = new HashSet(){{add(entry("key1", "value1"));}};
    verify(cacheLoaderWriter).writeAll(set);
    set = new HashSet(){{add(entry("key2", "value2"));}};
    verify(cacheLoaderWriter).writeAll(set);

    for (int i = 0; i < 3; i++) {
      assertThat(myCache.get("key" + i), is("value" + i));
    }

  }

  @Test
  public void testPutAll_with_cache_writer_that_throws_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriterThatThrows = mock(CacheLoaderWriter.class);
    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheLoaderWriterThatThrows).write(ArgumentMatchers
        .any(), ArgumentMatchers.any());
    doThrow(new Exception("Simulating an exception from the cache writer")).when(cacheLoaderWriterThatThrows).writeAll(ArgumentMatchers.any(Iterable.class));
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriterThatThrows);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider);
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.withLoaderWriter(cacheLoaderWriterThatThrows).build();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    try {
      myCache.putAll(stringStringHashMap);
      fail();
    } catch (BulkCacheWritingException bcwe) {
      assertThat(bcwe.getFailures().size(), is(3));
      assertThat(bcwe.getSuccesses().size(), is(0));
    }

  }


  @Test
  public void testPutAll_store_throws_cache_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .build();


    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriter = mock(CacheLoaderWriter.class);
    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheLoaderWriter).write(ArgumentMatchers.any(), ArgumentMatchers.any());
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriter);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    Map<String, String> stringStringHashMap = new HashMap<>();
    for (int i = 0; i < 3; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    myCache.putAll(stringStringHashMap);

    for (int i = 0; i < 3; i++) {
      // the store threw an exception when we call bulkCompute
      assertThat(myCache.get("key" + i), is(nullValue()));
      // but still, the cache writer could writeAll the values !
//      assertThat(cacheWriterToHashMapMap.get("key" + i), is("value" + i));
    }
    // but still, the cache writer could writeAll the values at once !
    verify(cacheLoaderWriter, times(1)).writeAll(ArgumentMatchers.any(Iterable.class));
    Set set = new HashSet() {{add(entry("key0", "value0")); add(entry("key1", "value1")); add(entry("key2", "value2"));}};
    verify(cacheLoaderWriter).writeAll(set);
  }

  private static Map.Entry entry(Object key, Object value) {
    return new AbstractMap.SimpleEntry(key, value);
  }

  @Test
  public void testGetAll_without_cache_loader() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.build();

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 3; i++) {
      myCache.put("key" + i, "value" + i);
    }

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key0");
        add("key2");
      }
    };
    // the call to getAll
    Map<String, String> fewEntries = myCache.getAll(fewKeysSet);

    assertThat(fewEntries.size(), is(2));
    assertThat(fewEntries.get("key0"), is("value0"));
    assertThat(fewEntries.get("key2"), is("value2"));

  }

  @Test
  public void testGetAll_with_cache_loader() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriter = mock(CacheLoaderWriter.class);
    when(cacheLoaderWriter.load(ArgumentMatchers.any())).thenThrow(new RuntimeException("We should not have called .load() but .loadAll()"));
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriter);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider);
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.withLoaderWriter(cacheLoaderWriter).build();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    when(cacheLoaderWriter.loadAll(argThat(IsCollectionContaining.<String>hasItem("key0")))).thenReturn(new HashMap(){{put("key0","value0");}});
    when(cacheLoaderWriter.loadAll(argThat(IsCollectionContaining.<String>hasItem("key2")))).thenReturn(new HashMap(){{put("key2","value2");}});

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key0");
        add("key2");
      }
    };
    // the call to getAll
    Map<String, String> fewEntries = myCache.getAll(fewKeysSet);

    assertThat(fewEntries.size(), is(2));
    assertThat(fewEntries.get("key0"), is("value0"));
    assertThat(fewEntries.get("key2"), is("value2"));

  }

  @Test
  public void testGetAll_cache_loader_throws_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriter = mock(CacheLoaderWriter.class);
    when(cacheLoaderWriter.load(ArgumentMatchers.any())).thenThrow(new RuntimeException("We should not have called .load() but .loadAll()"));
    when(cacheLoaderWriter.loadAll(ArgumentMatchers.any(Iterable.class))).thenThrow(new Exception("Simulating an exception from the cache loader"));
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriter);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider);
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.withLoaderWriter(cacheLoaderWriter).build();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key0");
        add("key2");
      }
    };


    // the call to getAll
    try {
      myCache.getAll(fewKeysSet);
      fail();
    } catch (BulkCacheLoadingException bcwe) {
      // since onHeapStore.bulkComputeIfAbsent sends batches of 1 element,
      assertThat(bcwe.getFailures().size(), is(2));
      assertThat(bcwe.getSuccesses().size(), is(0));
    }

  }

  @Test
  public void testGetAll_store_throws_cache_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.build();

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriter = mock(CacheLoaderWriter.class);
    when(cacheLoaderWriter.load(ArgumentMatchers.any())).thenThrow(new RuntimeException("We should not have called .load() but .loadAll()"));
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriter);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    when(cacheLoaderWriter.loadAll(argThat(hasItems("key0", "key2")))).thenReturn( new HashMap(){{put("key0","value0");  put("key2","value2");}});

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key0");
        add("key2");
      }
    };

    // the call to getAll
    Map<String, String> fewEntries = myCache.getAll(fewKeysSet);

    assertThat(fewEntries.size(), is(2));
    assertThat(fewEntries.get("key0"), is("value0"));
    assertThat(fewEntries.get("key2"), is("value2"));

  }

  @Test
  public void testRemoveAll_without_cache_writer() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.build();

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 3; i++) {
      myCache.put("key" + i, "value" + i);
    }

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key0");
        add("key2");
      }
    };
    // the call to removeAll
    myCache.removeAll(fewKeysSet);

    for (int i = 0; i < 3; i++) {
      if (i == 0 || i == 2) {
        assertThat(myCache.get("key" + i), is(nullValue()));
      } else {
        assertThat(myCache.get("key" + i), is("value" + i));
      }
    }

  }

  @Test
  public void testRemoveAll_with_cache_writer() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriter = mock(CacheLoaderWriter.class);
    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheLoaderWriter).delete(ArgumentMatchers.any());
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriter);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider);
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.withLoaderWriter(cacheLoaderWriter)
            .build();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 3; i++) {
      myCache.put("key" + i, "value" + i);
    }

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key0");
        add("key2");
      }
    };

    // the call to removeAll
    myCache.removeAll(fewKeysSet);

    for (int i = 0; i < 3; i++) {
      if (i == 0 || i == 2) {
        assertThat(myCache.get("key" + i), is(nullValue()));
      } else {
        assertThat(myCache.get("key" + i), is("value" + i));
      }
    }
    Set set = new HashSet(){{add("key0");}};
    verify(cacheLoaderWriter).deleteAll(set);
    set = new HashSet(){{add("key2");}};
    verify(cacheLoaderWriter).deleteAll(set);

  }


  @Test
  public void testRemoveAll_cache_writer_throws_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriterThatThrows = mock(CacheLoaderWriter.class);
    doThrow(new Exception("Simulating an exception from the cache writer")).when(cacheLoaderWriterThatThrows).deleteAll(ArgumentMatchers.any(Iterable.class));
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriterThatThrows);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider);
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.withLoaderWriter(cacheLoaderWriterThatThrows)
            .build();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 3; i++) {
      myCache.put("key" + i, "value" + i);
    }

    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheLoaderWriterThatThrows).write(ArgumentMatchers
        .any(), ArgumentMatchers.any());

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key0");
        add("key2");
      }
    };

    // the call to removeAll
    try {
      myCache.removeAll(fewKeysSet);
      fail();
    } catch (BulkCacheWritingException bcwe) {
      assertThat(bcwe.getFailures().size(), is(2));
      assertThat(bcwe.getSuccesses().size(), is(0));
    }

  }


  @Test
  public void testRemoveAll_with_store_that_throws() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        heap(100));
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .build();

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    CacheLoaderWriter cacheLoaderWriter = mock(CacheLoaderWriter.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), ArgumentMatchers.any(CacheConfiguration.class))).thenReturn(cacheLoaderWriter);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderWriterProvider).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build(true);

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 3; i++) {
      myCache.put("key" + i, "value" + i);
    }

    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheLoaderWriter).write(ArgumentMatchers
        .any(), ArgumentMatchers.any());

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key0");
        add("key2");
      }
    };

    // the call to removeAll
    myCache.removeAll(fewKeysSet);
    for (int i = 0; i < 3; i++) {
      if (i == 0 || i == 2) {
        assertThat(myCache.get("key" + i), is(nullValue()));

      } else {
        assertThat(myCache.get("key" + i), is("value" + i));
      }
    }

    Set set = new HashSet(){{add("key0"); add("key2");}};
    verify(cacheLoaderWriter).deleteAll(set);

  }

  /**
   * A Store provider that creates stores that throw...
   */
  private static class CustomStoreProvider implements Store.Provider {
    @Override
    public int rank(final Set<ResourceType<?>> resourceTypes, final Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
      return Integer.MAX_VALUE;     // Ensure this Store.Provider is ranked highest
    }

    @Override
    public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
      ServiceLocator serviceLocator = dependencySet().with(new DefaultSerializationProvider(null)).build();
      try {
        serviceLocator.startAllServices();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      final Copier defaultCopier = new IdentityCopier();
      return new OnHeapStore<K, V>(storeConfig, SystemTimeSource.INSTANCE, defaultCopier, defaultCopier,  new NoopSizeOfEngine(), NullStoreEventDispatcher.<K, V>nullStoreEventDispatcher(), new DefaultStatisticsService()) {
        @Override
        public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws StoreAccessException {
          throw new StoreAccessException("Problem trying to bulk compute");
        }

        @Override
        public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws StoreAccessException {
          throw new StoreAccessException("Problem trying to bulk compute");
        }
      };
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {

    }

    @Override
    public void initStore(Store<?, ?> resource) {

    }

    @Override
    public void start(final ServiceProvider<Service> serviceProvider) {

    }

    @Override
    public void stop() {

    }
  }

}
