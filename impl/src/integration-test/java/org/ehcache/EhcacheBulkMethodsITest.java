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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.exceptions.BulkCacheLoaderException;
import org.ehcache.exceptions.BulkCacheWriterException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.store.OnHeapStore;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loader.CacheLoader;
import org.ehcache.spi.loader.CacheLoaderFactory;
import org.ehcache.spi.service.CacheWriterConfiguration;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.writer.CacheWriter;
import org.ehcache.spi.writer.CacheWriterFactory;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Created by anthony on 2014-10-23.
 */
public class EhcacheBulkMethodsITest {

  @Test
  public void testPutAll_without_cache_writer() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildCacheConfig(String.class, String.class);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
    for (int i = 0; i < 100; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    myCache.putAll(stringStringHashMap.entrySet());

    for (int i = 0; i < 100; i++) {
      assertThat(myCache.get("key" + i), is("value" + i));
    }

  }

  @Test
  public void testPutAll_with_cache_writer() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .addServiceConfig(new CacheWriterConfiguration())
        .buildCacheConfig(String.class, String.class);

    CacheWriterFactoryHashMap cacheWriterFactoryHashMap = new CacheWriterFactoryHashMap(false);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactoryHashMap);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
    for (int i = 0; i < 100; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    myCache.putAll(stringStringHashMap.entrySet());

    Map<String, String> cacheWriterToHashMapMap = cacheWriterFactoryHashMap.getCacheWriterToHashMap().getMap();
    for (int i = 0; i < 100; i++) {
      assertThat(myCache.get("key" + i), is("value" + i));
      assertThat(cacheWriterToHashMapMap.get("key" + i), is("value" + i));
    }

  }

  @Test
  public void testPutAll_with_cache_writer_that_throws_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .addServiceConfig(new CacheWriterConfiguration())
        .buildCacheConfig(String.class, String.class);

    CacheWriterFactoryHashMap cacheWriterFactoryHashMap = new CacheWriterFactoryHashMap(true);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactoryHashMap);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
    for (int i = 0; i < 100; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    try {
      myCache.putAll(stringStringHashMap.entrySet());
      fail();
    } catch (BulkCacheWriterException bcwe) {
      assertThat(bcwe.getFailures().size(), is(100));
      assertThat(bcwe.getSuccesses().size(), is(0));
    }

  }


  @Test
  public void testPutAll_with_cache_writer_and_store_that_throws_cache_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .addServiceConfig(new CacheWriterConfiguration())
        .buildCacheConfig(String.class, String.class);

    CacheWriterFactoryHashMap cacheWriterFactoryHashMap = new CacheWriterFactoryHashMap(false);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactoryHashMap).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    HashMap<String, String> stringStringHashMap = new HashMap<String, String>();
    for (int i = 0; i < 100; i++) {
      stringStringHashMap.put("key" + i, "value" + i);
    }

    // the call to putAll
    myCache.putAll(stringStringHashMap.entrySet());
    Map<String, String> cacheWriterToHashMapMap = cacheWriterFactoryHashMap.getCacheWriterToHashMap().getMap();
    for (int i = 0; i < 100; i++) {
      // the store threw an exception when we call bulkCompute
      assertThat(myCache.get("key" + i), is(nullValue()));
      // but still, the cache writer could writeAll the values !
      assertThat(cacheWriterToHashMapMap.get("key" + i), is("value" + i));
    }
  }

  @Test
  public void testGetAll_without_cache_loader() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildCacheConfig(String.class, String.class);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 100; i++) {
      myCache.put("key" + i, "value" + i);
    }

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key12");
        add("key43");
      }
    };
    // the call to getAll
    Map<String, String> fewEntries = myCache.getAll(fewKeysSet);

    assertThat(fewEntries.size(), is(2));
    assertThat(fewEntries.get("key12"), is("value12"));
    assertThat(fewEntries.get("key43"), is("value43"));

  }

  @Test
  public void testGetAll_with_cache_loader() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildCacheConfig(String.class, String.class);

    CacheLoaderFactoryHashMap cacheLoaderFactoryHashMap = new CacheLoaderFactoryHashMap(false);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderFactoryHashMap);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();

    CacheLoaderFromHashMap cacheLoaderFromHashMap = cacheLoaderFactoryHashMap.getCacheLoaderFromHashMap();
    for (int i = 0; i < 100; i++) {
      cacheLoaderFromHashMap.addToMap("key" + i, "value" + i);
    }

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key12");
        add("key43");
      }
    };
    // the call to getAll
    Map<String, String> fewEntries = myCache.getAll(fewKeysSet);

    assertThat(fewEntries.size(), is(2));
    assertThat(fewEntries.get("key12"), is("value12"));
    assertThat(fewEntries.get("key43"), is("value43"));

  }

  @Test
  public void testGetAll_with_cache_loader_that_throws_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildCacheConfig(String.class, String.class);

    CacheLoaderFactoryHashMap cacheLoaderFactoryHashMap = new CacheLoaderFactoryHashMap(true);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderFactoryHashMap);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();

    CacheLoaderFromHashMap cacheLoaderFromHashMap = cacheLoaderFactoryHashMap.getCacheLoaderFromHashMap();
    for (int i = 0; i < 100; i++) {
      cacheLoaderFromHashMap.addToMap("key" + i, "value" + i);
    }

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key12");
        add("key43");
      }
    };


    // the call to getAll
    try {
      Map<String, String> fewEntries = myCache.getAll(fewKeysSet);
      fail();
    } catch (BulkCacheLoaderException bcwe) {
      assertThat(bcwe.getFailures().size(), is(2));
      assertThat(bcwe.getSuccesses().size(), is(0));
    }

  }

  @Test
  public void testGetAll_with_cache_loader_and_store_that_throws_cache_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildCacheConfig(String.class, String.class);

    CacheLoaderFactoryHashMap cacheLoaderFactoryHashMap = new CacheLoaderFactoryHashMap(false);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheLoaderFactoryHashMap).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();

    CacheLoaderFromHashMap cacheLoaderFromHashMap = cacheLoaderFactoryHashMap.getCacheLoaderFromHashMap();
    for (int i = 0; i < 100; i++) {
      cacheLoaderFromHashMap.addToMap("key" + i, "value" + i);
    }

    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key12");
        add("key43");
      }
    };

    // the call to getAll
    Map<String, String> fewEntries = myCache.getAll(fewKeysSet);

    assertThat(fewEntries.size(), is(2));
    assertThat(fewEntries.get("key12"), is("value12"));
    assertThat(fewEntries.get("key43"), is("value43"));

  }

  @Test
  public void testRemoveAll_without_cache_writer() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder.buildCacheConfig(String.class, String.class);

    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 100; i++) {
      myCache.put("key" + i, "value" + i);
    }

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key12");
        add("key43");
      }
    };
    // the call to removeAll
    myCache.removeAll(fewKeysSet);

    for (int i = 0; i < 100; i++) {
      if (i == 12 || i == 43) {
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
        .addServiceConfig(new CacheWriterConfiguration())
        .buildCacheConfig(String.class, String.class);

    CacheWriterFactoryHashMap cacheWriterFactoryHashMap = new CacheWriterFactoryHashMap(false);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactoryHashMap);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 100; i++) {
      myCache.put("key" + i, "value" + i);
    }

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key12");
        add("key43");
      }
    };

    // the call to removeAll
    myCache.removeAll(fewKeysSet);

    Map<String, String> cacheWriterToHashMapMap = cacheWriterFactoryHashMap.getCacheWriterToHashMap().getMap();
    for (int i = 0; i < 100; i++) {
      if (i == 12 || i == 43) {
        assertThat(myCache.get("key" + i), is(nullValue()));
        assertThat(cacheWriterToHashMapMap.get("key" + i), is(nullValue()));

      } else {
        assertThat(myCache.get("key" + i), is("value" + i));
        assertThat(cacheWriterToHashMapMap.get("key" + i), is("value" + i));
      }
    }

  }


  @Test
  public void testRemoveAll_with_cache_writer_that_throws_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .addServiceConfig(new CacheWriterConfiguration())
        .buildCacheConfig(String.class, String.class);

    CacheWriterFactoryHashMap cacheWriterFactoryHashMap = new CacheWriterFactoryHashMap(true);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactoryHashMap);
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 100; i++) {
      myCache.put("key" + i, "value" + i);
    }

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key12");
        add("key43");
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
  public void testRemoveAll_with_cache_writer_and_store_that_throws_cache_exception() throws Exception {
    CacheConfigurationBuilder cacheConfigurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    CacheConfiguration<String, String> cacheConfiguration = cacheConfigurationBuilder
        .addServiceConfig(new CacheWriterConfiguration())
        .buildCacheConfig(String.class, String.class);

    CacheWriterFactoryHashMap cacheWriterFactoryHashMap = new CacheWriterFactoryHashMap(false);
    CacheManagerBuilder<CacheManager> managerBuilder = CacheManagerBuilder.newCacheManagerBuilder().using(cacheWriterFactoryHashMap).using(new CustomStoreProvider());
    CacheManager cacheManager = managerBuilder.withCache("myCache", cacheConfiguration).build();


    Cache<String, String> myCache = cacheManager.getCache("myCache", String.class, String.class);

    for (int i = 0; i < 100; i++) {
      myCache.put("key" + i, "value" + i);
    }

    Set<String> fewKeysSet = new HashSet<String>() {
      {
        add("key12");
        add("key43");
      }
    };

    // the call to removeAll
    myCache.removeAll(fewKeysSet);
    for (int i = 0; i < 100; i++) {
      if (i == 12 || i == 43) {
        assertThat(myCache.get("key" + i), is(nullValue()));

      } else {
        assertThat(myCache.get("key" + i), is("value" + i));
      }
    }

  }

  private static class CacheWriterToHashMap<K, V> implements CacheWriter<K, V> {

    private boolean throwsException;

    public CacheWriterToHashMap(boolean throwsException) {
      this.throwsException = throwsException;
    }

    public Map<K, V> getMap() {
      return Collections.unmodifiableMap(map);
    }

    private ConcurrentMap<K, V> map = new ConcurrentHashMap<K, V>();

    @Override
    public void write(K key, V value) throws Exception {
      map.put(key, value);
    }

    @Override
    public boolean write(K key, V oldValue, V newValue) throws Exception {
      throw new RuntimeException("putAll should never call write, but writeAll");
    }

    @Override
    public Set<K> writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws Exception {
      if (throwsException) {
        throw new Exception();
      }
      Set keySet = new HashSet();
      for (Map.Entry<? extends K, ? extends V> entry : entries) {
        map.put(entry.getKey(), entry.getValue());
        keySet.add(entry.getKey());
      }
      return keySet;
    }

    @Override
    public boolean delete(K key) throws Exception {
      throw new RuntimeException("removeAll should never call delete, but deleteAll");
    }

    @Override
    public boolean delete(K key, V value) throws Exception {
      throw new RuntimeException("removeAll should never call delete, but deleteAll");
    }

    @Override
    public Set<K> deleteAll(Iterable<? extends K> keys) throws Exception {
      if (throwsException) {
        throw new Exception();
      }
      Set keySet = new HashSet();
      for (K key : keys) {
        keySet.add(key);
        map.remove(key);
      }
      return keySet;
    }
  }

  private static class CacheWriterFactoryHashMap implements CacheWriterFactory {

    private boolean throwsException;

    private CacheWriterToHashMap cacheWriterToHashMap;

    public CacheWriterFactoryHashMap(boolean throwsException) {
      this.throwsException = throwsException;
    }

    public CacheWriterToHashMap getCacheWriterToHashMap() {
      return cacheWriterToHashMap;
    }

    @Override
    public <K, V> CacheWriter<? super K, ? super V> createCacheWriter(String alias, CacheConfiguration<K, V> cacheConfiguration) {
      cacheWriterToHashMap = new CacheWriterToHashMap<K, V>(throwsException);
      return cacheWriterToHashMap;
    }

    @Override
    public void releaseCacheWriter(CacheWriter<?, ?> cacheWriter) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
  }

  private static class CustomStoreProvider implements Store.Provider {
    @Override
    public <K, V> Store<K, V> createStore(Store.Configuration<K, V> storeConfig, ServiceConfiguration<?>... serviceConfigs) {
      return new OnHeapStore<K, V>(storeConfig) {
        @Override
        public Map<K, ValueHolder<V>> bulkCompute(Iterable<? extends K> keys, Function<Iterable<? extends Map.Entry<? extends K, ? extends V>>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> remappingFunction) throws CacheAccessException {
          throw new CacheAccessException("Problem trying to bulk compute");
        }

        @Override
        public Map<K, ValueHolder<V>> bulkComputeIfAbsent(Iterable<? extends K> keys, Function<Iterable<? extends K>, Iterable<? extends Map.Entry<? extends K, ? extends V>>> mappingFunction) throws CacheAccessException {
          throw new CacheAccessException("Problem trying to bulk compute");
        }
      };
    }

    @Override
    public void releaseStore(Store<?, ?> resource) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
  }

  private class CacheLoaderFactoryHashMap implements CacheLoaderFactory {


    private boolean throwsException;

    public CacheLoaderFromHashMap getCacheLoaderFromHashMap() {
      return cacheLoaderFromHashMap;
    }

    private CacheLoaderFromHashMap cacheLoaderFromHashMap;

    public CacheLoaderFactoryHashMap(boolean throwsException) {
      this.throwsException = throwsException;
    }

    @Override
    public <K, V> CacheLoader<? super K, ? extends V> createCacheLoader(String alias, CacheConfiguration<K, V> cacheConfiguration) {
      cacheLoaderFromHashMap = new CacheLoaderFromHashMap<K, V>(throwsException);
      return cacheLoaderFromHashMap;
    }

    @Override
    public void releaseCacheLoader(CacheLoader<?, ?> cacheLoader) {

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }
  }


  private static class CacheLoaderFromHashMap<K, V> implements CacheLoader<K, V> {

    private boolean throwsException;
    private final ConcurrentMap<K, V> map = new ConcurrentHashMap<K, V>();

    public CacheLoaderFromHashMap(boolean throwsException) {
      this.throwsException = throwsException;
    }

    @Override
    public V load(K key) throws Exception {
      throw new RuntimeException("getAll should never call load, but loadAll");
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
      if (throwsException) {
        throw new Exception();
      }
      Map<K, V> mapToReturn = new HashMap<K, V>();
      for (K key : keys) {
        mapToReturn.put(key, map.get(key));
      }
      return mapToReturn;
    }

    public Map<K, V> addToMap(K key, V value) {
      map.put(key, value);
      return map;
    }
  }

}
