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
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.exceptions.BulkCacheLoaderException;
import org.ehcache.exceptions.BulkCacheWriterException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.internal.SystemTimeSource;
import org.ehcache.internal.store.OnHeapStore;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.spi.writer.CacheWriterFactory;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Created by anthony on 2014-10-23.
 */
@SuppressWarnings({ "unchecked", "rawtypes", "serial" })
public class EhcacheBulkMethodsITest {

  @Test
  public void testPutAll_without_cache_writer() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildConfig(String.class, String.class);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
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
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .buildConfig(String.class, String.class);

    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    CacheWriter cacheWriter = mock(CacheWriter.class);
    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheWriter).write(Matchers
        .anyObject(), Matchers.anyObject());
    when(cacheWriterFactory.createCacheWriter(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheWriter);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactory);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
    for (int i = 0; i < 3; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    myCache.putAll(stringStringHashMap);

    verify(cacheWriter, times(3)).writeAll(Matchers.any(Iterable.class));
    Set set = new HashSet(){{add(entry("key0", "value0"));}};
    verify(cacheWriter).writeAll(set);
    set = new HashSet(){{add(entry("key1", "value1"));}};
    verify(cacheWriter).writeAll(set);
    set = new HashSet(){{add(entry("key2", "value2"));}};
    verify(cacheWriter).writeAll(set);

    for (int i = 0; i < 3; i++) {
      assertThat(myCache.get("key" + i), is("value" + i));
    }

  }

  @Test
  public void testPutAll_with_cache_writer_that_throws_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .buildConfig(String.class, String.class);

    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    CacheWriter cacheWriterThatThrows = mock(CacheWriter.class);
    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheWriterThatThrows).write(Matchers
        .anyObject(), Matchers.anyObject());
    doThrow(new Exception("Simulating an exception from the cache writer")).when(cacheWriterThatThrows).writeAll(Matchers.any(Iterable.class));
    when(cacheWriterFactory.createCacheWriter(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheWriterThatThrows);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactory);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
    for (int i = 0; i < 3; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    try {
      myCache.putAll(stringStringHashMap);
      fail();
    } catch (BulkCacheWriterException bcwe) {
      assertThat(bcwe.getFailures().size(), is(3));
      assertThat(bcwe.getSuccesses().size(), is(0));
    }

  }


  @Test
  public void testPutAll_store_throws_cache_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .buildConfig(String.class, String.class);


    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    CacheWriter cacheWriter = mock(CacheWriter.class);
    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheWriter).write(Matchers.anyObject(), Matchers.anyObject());
    when(cacheWriterFactory.createCacheWriter(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheWriter);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactory).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    Map<String, String> stringStringHashMap = new HashMap<String, String>();
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
    verify(cacheWriter, times(1)).writeAll(Matchers.any(Iterable.class));
    Set set = new HashSet() {{add(entry("key0", "value0")); add(entry("key1", "value1")); add(entry("key2", "value2"));}};
    verify(cacheWriter).writeAll(set);
  }

  private static Map.Entry entry(Object key, Object value) {
    return new AbstractMap.SimpleEntry(key, value);
  }
  
  @Test
  public void testGetAll_without_cache_loader() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildConfig(String.class, String.class);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


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
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildConfig(String.class, String.class);

    CacheLoaderFactory cacheLoaderFactory = mock(CacheLoaderFactory.class);
    CacheLoader cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(Matchers.anyObject())).thenThrow(new RuntimeException("We should not have called .load() but .loadAll()"));
    when(cacheLoaderFactory.createCacheLoader(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheLoader);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderFactory);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();

    when(cacheLoader.loadAll(argThat(hasItem("key0")))).thenReturn( new HashMap(){{put("key0","value0");}});
    when(cacheLoader.loadAll(argThat(hasItem("key2")))).thenReturn( new HashMap(){{put("key2","value2");}});

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
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildConfig(String.class, String.class);

    CacheLoaderFactory cacheLoaderFactory = mock(CacheLoaderFactory.class);
    CacheLoader cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(Matchers.anyObject())).thenThrow(new RuntimeException("We should not have called .load() but .loadAll()"));
    when(cacheLoader.loadAll(Matchers.any(Iterable.class))).thenThrow(new Exception("Simulating an exception from the cache loader"));
    when(cacheLoaderFactory.createCacheLoader(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheLoader);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderFactory);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();

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
    } catch (BulkCacheLoaderException bcwe) {
      // since onHeapStore.bulkComputeIfAbsent sends batches of 1 element,
      assertThat(bcwe.getFailures().size(), is(2));
      assertThat(bcwe.getSuccesses().size(), is(0));
    }

  }

  @Test
  public void testGetAll_store_throws_cache_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildConfig(String.class, String.class);

    CacheLoaderFactory cacheLoaderFactory = mock(CacheLoaderFactory.class);
    CacheLoader cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(Matchers.anyObject())).thenThrow(new RuntimeException("We should not have called .load() but .loadAll()"));
    when(cacheLoaderFactory.createCacheLoader(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheLoader);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderFactory).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();

    when(cacheLoader.loadAll(argThat(hasItems("key0", "key2")))).thenReturn( new HashMap(){{put("key0","value0");  put("key2","value2");}});

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
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildConfig(String.class, String.class);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


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
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .buildConfig(String.class, String.class);

    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    CacheWriter cacheWriter = mock(CacheWriter.class);
    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheWriter).delete(Matchers.anyObject());
    when(cacheWriterFactory.createCacheWriter(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheWriter);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactory);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


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
    verify(cacheWriter).deleteAll(set);
    set = new HashSet(){{add("key2");}};
    verify(cacheWriter).deleteAll(set);

  }


  @Test
  public void testRemoveAll_cache_writer_throws_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .buildConfig(String.class, String.class);

    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    CacheWriter cacheWriterThatThrows = mock(CacheWriter.class);
    doThrow(new Exception("Simulating an exception from the cache writer")).when(cacheWriterThatThrows).deleteAll(Matchers.any(Iterable.class));
    when(cacheWriterFactory.createCacheWriter(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheWriterThatThrows);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactory);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 3; i++) {
      myCache.put("key" + i, "value" + i);
    }

    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheWriterThatThrows).write(Matchers
        .anyObject(), Matchers.anyObject());

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
    } catch (BulkCacheWriterException bcwe) {
      assertThat(bcwe.getFailures().size(), is(2));
      assertThat(bcwe.getSuccesses().size(), is(0));
    }

  }


  @Test
  public void testRemoveAll_with_store_that_throws() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .buildConfig(String.class, String.class);

    CacheWriterFactory cacheWriterFactory = mock(CacheWriterFactory.class);
    CacheWriter cacheWriter = mock(CacheWriter.class);
    when(cacheWriterFactory.createCacheWriter(anyString(), Matchers.any(CacheConfiguration.class))).thenReturn(cacheWriter);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactory).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 3; i++) {
      myCache.put("key" + i, "value" + i);
    }

    doThrow(new RuntimeException("We should not have called .write() but .writeAll()")).when(cacheWriter).write(Matchers
        .anyObject(), Matchers.anyObject());

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
    verify(cacheWriter).deleteAll(set);

  }

  /**
   * A Store provider that creates stores that throw...
   */
  private static class CustomStoreProvider implements Store.Provider {
    @Override
    public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return new OnHeapStore<K, V>(storeConfig, SystemTimeSource.INSTANCE, false) {
        @Override
        public Map<K, ValueHolder<V>> bulkCompute(Set<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
          throw new CacheAccessException("Problem trying to bulk compute");
        }

        @Override
        public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Set<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
          throw new CacheAccessException("Problem trying to bulk compute");
        }
      };
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {

    }

    @Override
    public void start(ServiceConfiguration<?> config) {

    }

    @Override
    public void stop() {

    }
  }

}
