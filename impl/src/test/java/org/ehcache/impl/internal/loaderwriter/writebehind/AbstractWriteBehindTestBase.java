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
package org.ehcache.impl.internal.loaderwriter.writebehind;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeoutException;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration;
import static org.ehcache.config.builders.WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Abhilash
 *
 */
public abstract class AbstractWriteBehindTestBase {

  protected abstract CacheManagerBuilder managerBuilder();

  protected abstract CacheConfigurationBuilder<String, String> configurationBuilder();

  @Test
  public void testWriteOrdering() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testWriteOrdering", configurationBuilder()
          .add(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 8).build())
          .build());

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

      countDownLatch.await(4, SECONDS);

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

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testWrites", CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, heap(10))
          .add(newUnBatchedWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
          .build());

      CountDownLatch countDownLatch = new CountDownLatch(4);
      loaderWriter.setLatch(countDownLatch);
      testCache.put("test1", "test1");
      testCache.put("test2", "test2");
      testCache.put("test3", "test3");
      testCache.remove("test2");

      countDownLatch.await(2, SECONDS);

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

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testBulkWrites", CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, heap(100))
          .add(newUnBatchedWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
          .build());

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
      countDownLatch.await(5, SECONDS);
      for (int i = 0; i < 20; i++) {
        assertThat("Key : " + i, loaderWriter.getData().get("test" + i), contains("test" + i));
      }

      CountDownLatch countDownLatch1 = new CountDownLatch(10);
      loaderWriter.setLatch(countDownLatch1);
      testCache.removeAll(keys);

      countDownLatch1.await(5, SECONDS);

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


    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testThatAllGetsReturnLatestData", configurationBuilder()
          .add(newUnBatchedWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
          .build());

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

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testAllGetsReturnLatestDataWithKeyCollision", configurationBuilder()
          .add(newUnBatchedWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
          .build());

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

  @Test
  public void testBatchedDeletedKeyReturnsNull() throws Exception {
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(loaderWriter.load("key")).thenReturn("value");
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testBatchedDeletedKeyReturnsNull", configurationBuilder()
          .add(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).build())
          .build());

      assertThat(testCache.get("key"), is("value"));

      testCache.remove("key");

      assertThat(testCache.get("key"), nullValue());
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testUnBatchedDeletedKeyReturnsNull() throws Exception {
    final Semaphore semaphore = new Semaphore(0);

    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(loaderWriter.load("key")).thenReturn("value");
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        semaphore.acquire();
        return null;
      }
    }).when(loaderWriter).delete("key");
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testUnBatchedDeletedKeyReturnsNull", configurationBuilder()
          .add(newUnBatchedWriteBehindConfiguration().build())
          .build());

      assertThat(testCache.get("key"), is("value"));

      testCache.remove("key");

      assertThat(testCache.get("key"), nullValue());
    } finally {
      semaphore.release();
      cacheManager.close();
    }
  }

  @Test
  public void testBatchedOverwrittenKeyReturnsNewValue() throws Exception {
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(loaderWriter.load("key")).thenReturn("value");
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testBatchedOverwrittenKeyReturnsNewValue", configurationBuilder()
          .add(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).build())
          .build());

      assertThat(testCache.get("key"), is("value"));

      testCache.put("key", "value2");

      assertThat(testCache.get("key"), is("value2"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testUnBatchedOverwrittenKeyReturnsNewValue() throws Exception {
    final Semaphore semaphore = new Semaphore(0);

    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(loaderWriter.load("key")).thenReturn("value");
    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        semaphore.acquire();
        return null;
      }
    }).when(loaderWriter).delete("key");
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testUnBatchedOverwrittenKeyReturnsNewValue", configurationBuilder()
          .add(newUnBatchedWriteBehindConfiguration().build())
          .build());

      assertThat(testCache.get("key"), is("value"));

      testCache.remove("key");

      assertThat(testCache.get("key"), nullValue());
    } finally {
      semaphore.release();
      cacheManager.close();
    }
  }

  @Test
  public void testCoaslecedWritesAreNotSeen() throws InterruptedException {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testCoaslecedWritesAreNotSeen", configurationBuilder()
          .add(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).enableCoalescing().build())
          .build());

      CountDownLatch latch = new CountDownLatch(2);
      loaderWriter.setLatch(latch);

      testCache.put("keyA", "value1");
      testCache.put("keyA", "value2");

      testCache.put("keyB", "value3");

      latch.await();
      assertThat(loaderWriter.getValueList("keyA"), contains("value2"));
      assertThat(loaderWriter.getValueList("keyB"), contains("value3"));
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testUnBatchedWriteBehindStopWaitsForEmptyQueue() {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testUnBatchedWriteBehindStopWaitsForEmptyQueue", configurationBuilder()
          .add(newUnBatchedWriteBehindConfiguration().build())
          .build());

      testCache.put("key", "value");
    } finally {
      cacheManager.close();
    }
    assertThat(loaderWriter.getValueList("key"), contains("value"));
  }

  @Test
  public void testBatchedWriteBehindStopWaitsForEmptyQueue() {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testBatchedWriteBehindStopWaitsForEmptyQueue", configurationBuilder()
          .add(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).build())
          .build());

      testCache.put("key", "value");
    } finally {
      cacheManager.close();
    }
    assertThat(loaderWriter.getValueList("key"), contains("value"));
  }

  @Test
  public void testUnBatchedWriteBehindBlocksWhenFull() throws Exception {
    final Semaphore gate = new Semaphore(0);
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    doAnswer(new Answer() {

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        gate.acquire();
        return null;
      }
    }).when(loaderWriter).write(anyString(), anyString());

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      final Cache<String, String> testCache = cacheManager.createCache("testUnBatchedWriteBehindBlocksWhenFull", configurationBuilder()
          .add(newUnBatchedWriteBehindConfiguration().queueSize(1).build())
          .build());

      testCache.put("key1", "value");
      testCache.put("key2", "value");

      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> blockedPut = executor.submit(new Runnable() {

          @Override
          public void run() {
            testCache.put("key3", "value");
          }
        });

        try {
          blockedPut.get(100, MILLISECONDS);
          fail("Expected TimeoutException");
        } catch (TimeoutException e) {
          //expected
        }
        gate.release();
        blockedPut.get(10, SECONDS);
        gate.release(Integer.MAX_VALUE);
      } finally {
        executor.shutdown();
      }
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testBatchedWriteBehindBlocksWhenFull() throws Exception {
    final Semaphore gate = new Semaphore(0);
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    doAnswer(new Answer() {

      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        gate.acquire();
        return null;
      }
    }).when(loaderWriter).writeAll(any(Iterable.class));

    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      final Cache<String, String> testCache = cacheManager.createCache("testBatchedWriteBehindBlocksWhenFull", configurationBuilder()
          .add(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 1).queueSize(1).build())
          .build());

      testCache.put("key1", "value");
      testCache.put("key2", "value");

      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> blockedPut = executor.submit(new Runnable() {

          @Override
          public void run() {
            testCache.put("key3", "value");
          }
        });

        try {
          blockedPut.get(100, MILLISECONDS);
          fail("Expected TimeoutException");
        } catch (TimeoutException e) {
          //expected
        }
        gate.release();
        blockedPut.get(10, SECONDS);
        gate.release(Integer.MAX_VALUE);
      } finally {
        executor.shutdown();
      }
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testFilledBatchedIsWritten() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testFilledBatchedIsWritten", configurationBuilder()
          .add(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).build())
          .build());

      CountDownLatch latch = new CountDownLatch(2);
      loaderWriter.setLatch(latch);

      testCache.put("key1", "value");
      testCache.put("key2", "value");

      if (latch.await(10, SECONDS)) {
        assertThat(loaderWriter.getValueList("key1"), contains("value"));
        assertThat(loaderWriter.getValueList("key2"), contains("value"));
      } else {
        fail("Took too long to write, assuming batch is not going to be written");
      }
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testAgedBatchedIsWritten() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)anyObject())).thenReturn((CacheLoaderWriter)loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testAgedBatchedIsWritten", configurationBuilder()
          .add(newBatchedWriteBehindConfiguration(1, SECONDS, 2).build())
          .build());

      CountDownLatch latch = new CountDownLatch(1);
      loaderWriter.setLatch(latch);

      testCache.put("key1", "value");

      if (latch.await(10, SECONDS)) {
        assertThat(loaderWriter.getValueList("key1"), contains("value"));
      } else {
        fail("Took too long to write, assuming batch is not going to be written");
      }
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void testWriteBehindQueueSize() throws Exception {

    class TestWriteBehindProvider extends WriteBehindProviderFactory.Provider {

      private WriteBehind writeBehind = null;

      @Override
      public <K, V> WriteBehind<K, V> createWriteBehindLoaderWriter(final CacheLoaderWriter<K, V> cacheLoaderWriter, final WriteBehindConfiguration configuration) {
        this.writeBehind = super.createWriteBehindLoaderWriter(cacheLoaderWriter, configuration);
        return writeBehind;
      }

      public WriteBehind getWriteBehind() {
        return writeBehind;
      }
    }

    TestWriteBehindProvider writeBehindProvider = new TestWriteBehindProvider();
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<String, String>();

    CacheManager cacheManager = managerBuilder().using(writeBehindProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testAgedBatchedIsWritten", configurationBuilder()
          .add(new DefaultCacheLoaderWriterConfiguration(loaderWriter))
          .add(newBatchedWriteBehindConfiguration(5, SECONDS, 2).build())
          .build());

      testCache.put("key1", "value1");
      assertThat(writeBehindProvider.getWriteBehind().getQueueSize(), is(1L));
      testCache.put("key2", "value2");

    } finally {
      cacheManager.close();
    }
  }
}
