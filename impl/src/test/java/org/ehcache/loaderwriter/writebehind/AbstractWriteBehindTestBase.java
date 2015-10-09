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
package org.ehcache.loaderwriter.writebehind;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.junit.Test;

import static org.ehcache.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.writebehind.WriteBehindConfigurationBuilder.newWriteBehindConfiguration;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Abhilash
 *
 */
public abstract class AbstractWriteBehindTestBase {
  
  protected abstract CacheConfigurationBuilder<Object, Object> configurationBuilder();

  @Test
  public void testWriteOrdering() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);
    
    CacheManager cacheManager = newCacheManagerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testWriteOrdering", configurationBuilder()
          .add(newWriteBehindConfiguration().batchSize(8).build())
          .buildConfig(String.class, String.class));

      CountDownLatch countDownLatch = new CountDownLatch(8);

      loaderWriter.setLatch(countDownLatch);
      
      testCache.remove("key");
      testCache.put("key", "value1");
      testCache.remove("key");
      testCache.put("key", "value2");
      testCache.remove("key");
      testCache.put("key", "value3");
      testCache.remove("key");
      testCache.put("key", "value4");

      countDownLatch.await(4, TimeUnit.SECONDS);

      assertThat(loaderWriter.getData().get("key"), contains(null, "value1", null, "value2", null, "value3", null, "value4"));
    } finally {
      cacheManager.close();
    }
  }
  
  @Test
  public void testWrites() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);
    
    CacheManager cacheManager = newCacheManagerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testWrites", CacheConfigurationBuilder.newCacheConfigurationBuilder()
          .add(newWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
          .buildConfig(String.class, String.class));

      CountDownLatch countDownLatch = new CountDownLatch(4);
      loaderWriter.setLatch(countDownLatch);
      testCache.put("test1", "test1");
      testCache.put("test2", "test2");
      testCache.put("test3", "test3");
      testCache.remove("test2");

      countDownLatch.await(2, TimeUnit.SECONDS);

      assertThat(loaderWriter.getData().get("test1"), contains("test1"));
      assertThat(loaderWriter.getData().get("test2"), contains("test2", null));
      assertThat(loaderWriter.getData().get("test3"), contains("test3"));
    } finally {
      cacheManager.close();
    }
  }
  
  @Test
  public void testBulkWrites() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);
    
    CacheManager cacheManager = newCacheManagerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testBulkWrites", CacheConfigurationBuilder.newCacheConfigurationBuilder()
          .add(newWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
          .buildConfig(String.class, String.class));

      CountDownLatch countDownLatch = new CountDownLatch(20);
      loaderWriter.setLatch(countDownLatch);
      for(int i=0 ; i<10; i++)
        testCache.put("test"+i, "test"+i);

      Map<String, String> entries = new HashMap<String, String>(10);
      Set<String> keys = new HashSet<String>(10);
      for(int i=10 ; i<20; i++) {
        entries.put("test"+i, "test"+i);
        keys.add("test"+i);
      }

      testCache.putAll(entries);
      countDownLatch.await(5, TimeUnit.SECONDS);
      for (int i = 0; i < 20; i++) {
        assertThat("Key : " + i, loaderWriter.getData().get("test" + i), contains("test" + i));
      }

      CountDownLatch countDownLatch1 = new CountDownLatch(10);
      loaderWriter.setLatch(countDownLatch1);
      testCache.removeAll(keys);

      countDownLatch1.await(5, TimeUnit.SECONDS);

      assertThat(loaderWriter.getData().size(), is(20));
      for (int i = 0; i < 10; i++) {
        assertThat("Key : " + i, loaderWriter.getData().get("test" + i), contains("test" + i));
      }
      for (int i = 10; i < 20; i++) {
        assertThat("Key : " + i, loaderWriter.getData().get("test" + i), contains("test" + i, null));
      }
    } finally {
      cacheManager.close();
    }
  }
  
  @Test
  public void testThatAllGetsReturnLatestData() throws BulkCacheWritingException, Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);
    

    CacheManager cacheManager = newCacheManagerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testThatAllGetsReturnLatestData", configurationBuilder()
          .add(newWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
          .buildConfig(String.class, String.class));

      for(int i=0 ; i<10; i++) {
        String val = "test"+i; 
        testCache.put(val, val );
      }
      testCache.remove("test8");

      assertThat(testCache.get("test8"), nullValue());

      for(int i=10; i<30; i++){
        String val = "test"+i; 
        testCache.put(val, val);
      }

      assertThat(testCache.get("test29"), is("test29"));    

      testCache.remove("test19");
      testCache.remove("test1");



      assertThat(testCache.get("test19"), nullValue());
      assertThat(testCache.get("test1"), nullValue());

      testCache.put("test11", "test11New");

      assertThat(testCache.get("test11"), is("test11New"));

      testCache.put("test7", "test7New");


      assertThat(testCache.get("test7"), is("test7New"));
    } finally {
      cacheManager.close();
    }
  }
  
  @Test
  public void testAllGetsReturnLatestDataWithKeyCollision() {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);
    
    CacheManager cacheManager = newCacheManagerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testAllGetsReturnLatestDataWithKeyCollision", configurationBuilder()
          .add(newWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
          .buildConfig(String.class, String.class));

      Random random = new Random();
      Set<String> keys = new HashSet<String>();
      for(int i = 0; i< 40; i++) {
        int index = random.nextInt(15);
        String key = "key"+ index;
        testCache.put(key, key);
        keys.add(key);
      }
      for (String key : keys) {
        testCache.put(key, key + "new");
      }
      for (String key : keys) {
        assertThat(testCache.get(key), is(key + "new"));
      }
    } finally {
      cacheManager.close();
    }
  }
}
