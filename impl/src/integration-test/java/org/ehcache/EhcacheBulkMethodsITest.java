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
import org.ehcache.exceptions.BulkCacheWriterException;
import org.ehcache.exceptions.CacheAccessException;
import org.ehcache.function.Function;
import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.store.OnHeapStore;
import org.ehcache.spi.cache.Store;
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
      return map.remove(key) != null ? true : false;
    }

    @Override
    public boolean delete(K key, V value) throws Exception {
      return map.remove(key, value);
    }

    @Override
    public Set<K> deleteAll(Iterable<? extends K> keys) throws Exception {
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
}
