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
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.junit.Test;

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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Abhilash
 *
 */
public abstract class AbstractWriteBehindTestBase {

  protected abstract CacheManagerBuilder<CacheManager> managerBuilder();

  protected abstract CacheConfigurationBuilder<String, String> configurationBuilder();

  @Test
  public void testWriteOrdering() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testWriteOrdering", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 8).build())
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

      assertThat(loaderWriter.getData()
        .get("key"), contains(null, "value1", null, "value2", null, "value3", null, "value4"));
    }
  }

  @Test
  public void testWrites() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testWrites", CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, heap(10))
        .withLoaderWriter(loaderWriter)
        .withService(newUnBatchedWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
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
    }
  }

  @Test
  public void testBulkWrites() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testBulkWrites", CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class, heap(100))
        .withLoaderWriter(loaderWriter)
        .withService(newUnBatchedWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
        .build());

      CountDownLatch countDownLatch = new CountDownLatch(20);
      loaderWriter.setLatch(countDownLatch);
      for (int i = 0; i < 10; i++)
        testCache.put("test" + i, "test" + i);

      Map<String, String> entries = new HashMap<>(10);
      Set<String> keys = new HashSet<>(10);
      for (int i = 10; i < 20; i++) {
        entries.put("test" + i, "test" + i);
        keys.add("test" + i);
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
    }
  }

  @Test
  public void testThatAllGetsReturnLatestData() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);


    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testThatAllGetsReturnLatestData", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newUnBatchedWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
        .build());

      for (int i = 0; i < 10; i++) {
        String val = "test" + i;
        testCache.put(val, val);
      }
      testCache.remove("test8");

      assertThat(testCache.get("test8"), nullValue());

      for (int i = 10; i < 30; i++) {
        String val = "test" + i;
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
    }
  }

  @Test
  public void testAllGetsReturnLatestDataWithKeyCollision() {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testAllGetsReturnLatestDataWithKeyCollision", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newUnBatchedWriteBehindConfiguration().concurrencyLevel(3).queueSize(10).build())
        .build());

      Random random = new Random();
      Set<String> keys = new HashSet<>();
      for (int i = 0; i < 40; i++) {
        int index = random.nextInt(15);
        String key = "key" + index;
        testCache.put(key, key);
        keys.add(key);
      }
      for (String key : keys) {
        testCache.put(key, key + "new");
      }
      for (String key : keys) {
        assertThat(testCache.get(key), is(key + "new"));
      }
    }
  }

  @Test
  public void testBatchedDeletedKeyReturnsNull() throws Exception {
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(loaderWriter.load("key")).thenReturn("value");
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testBatchedDeletedKeyReturnsNull", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).build())
        .build());

      assertThat(testCache.get("key"), is("value"));

      testCache.remove("key");

      assertThat(testCache.get("key"), nullValue());
    }
  }

  @Test
  public void testUnBatchedDeletedKeyReturnsNull() throws Exception {
    Semaphore semaphore = new Semaphore(0);

    @SuppressWarnings("unchecked")
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(loaderWriter.load("key")).thenReturn("value");
    doAnswer(invocation -> {
      semaphore.acquire();
      return null;
    }).when(loaderWriter).delete("key");
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testUnBatchedDeletedKeyReturnsNull", configurationBuilder()
          .withLoaderWriter(loaderWriter)
          .withService(newUnBatchedWriteBehindConfiguration().build())
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
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(loaderWriter.load("key")).thenReturn("value");
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testBatchedOverwrittenKeyReturnsNewValue", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).build())
        .build());

      assertThat(testCache.get("key"), is("value"));

      testCache.put("key", "value2");

      assertThat(testCache.get("key"), is("value2"));
    }
  }

  @Test
  public void testUnBatchedOverwrittenKeyReturnsNewValue() throws Exception {
    final Semaphore semaphore = new Semaphore(0);

    @SuppressWarnings("unchecked")
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    when(loaderWriter.load("key")).thenReturn("value");
    doAnswer(invocation -> {
      semaphore.acquire();
      return null;
    }).when(loaderWriter).delete("key");
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true);
    try {
      Cache<String, String> testCache = cacheManager.createCache("testUnBatchedOverwrittenKeyReturnsNewValue", configurationBuilder()
          .withLoaderWriter(loaderWriter)
          .withService(newUnBatchedWriteBehindConfiguration().build())
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
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testCoaslecedWritesAreNotSeen", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).enableCoalescing().build())
        .build());

      CountDownLatch latch = new CountDownLatch(2);
      loaderWriter.setLatch(latch);

      testCache.put("keyA", "value1");
      testCache.put("keyA", "value2");

      testCache.put("keyB", "value3");

      latch.await();
      assertThat(loaderWriter.getValueList("keyA"), contains("value2"));
      assertThat(loaderWriter.getValueList("keyB"), contains("value3"));
    }
  }

  @Test
  public void testUnBatchedWriteBehindStopWaitsForEmptyQueue() {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testUnBatchedWriteBehindStopWaitsForEmptyQueue", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newUnBatchedWriteBehindConfiguration().build())
        .build());

      testCache.put("key", "value");
    }
    assertThat(loaderWriter.getValueList("key"), contains("value"));
  }

  @Test
  public void testBatchedWriteBehindStopWaitsForEmptyQueue() {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testBatchedWriteBehindStopWaitsForEmptyQueue", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).build())
        .build());

      testCache.put("key", "value");
    }
    assertThat(loaderWriter.getValueList("key"), contains("value"));
  }

  @Test
  public void testUnBatchedWriteBehindBlocksWhenFull() throws Exception {
    final Semaphore gate = new Semaphore(0);
    @SuppressWarnings("unchecked")
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    doAnswer(invocation -> {
      gate.acquire();
      return null;
    }).when(loaderWriter).write(anyString(), anyString());

    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      final Cache<String, String> testCache = cacheManager.createCache("testUnBatchedWriteBehindBlocksWhenFull", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newUnBatchedWriteBehindConfiguration().queueSize(1).build())
        .build());

      testCache.put("key1", "value");
      testCache.put("key2", "value");

      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> blockedPut = executor.submit(() -> testCache.put("key3", "value"));

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
    }
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testBatchedWriteBehindBlocksWhenFull() throws Exception {
    final Semaphore gate = new Semaphore(0);
    CacheLoaderWriter<String, String> loaderWriter = mock(CacheLoaderWriter.class);
    doAnswer(invocation -> {
      gate.acquire();
      return null;
    }).when(loaderWriter).writeAll(any(Iterable.class));

    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      final Cache<String, String> testCache = cacheManager.createCache("testBatchedWriteBehindBlocksWhenFull", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 1).queueSize(1).build())
        .build());

      testCache.put("key1", "value");
      testCache.put("key2", "value");

      ExecutorService executor = Executors.newSingleThreadExecutor();
      try {
        Future<?> blockedPut = executor.submit(() -> testCache.put("key3", "value"));

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
    }
  }

  @Test
  public void testFilledBatchedIsWritten() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testFilledBatchedIsWritten", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newBatchedWriteBehindConfiguration(Long.MAX_VALUE, SECONDS, 2).build())
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
    }
  }

  @Test
  public void testAgedBatchedIsWritten() throws Exception {
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();
    CacheLoaderWriterProvider cacheLoaderWriterProvider = getMockedCacheLoaderWriterProvider(loaderWriter);

    try (CacheManager cacheManager = managerBuilder().using(cacheLoaderWriterProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testAgedBatchedIsWritten", configurationBuilder()
        .withLoaderWriter(loaderWriter)
        .withService(newBatchedWriteBehindConfiguration(1, SECONDS, 2).build())
        .build());

      CountDownLatch latch = new CountDownLatch(1);
      loaderWriter.setLatch(latch);

      testCache.put("key1", "value");

      if (latch.await(10, SECONDS)) {
        assertThat(loaderWriter.getValueList("key1"), contains("value"));
      } else {
        fail("Took too long to write, assuming batch is not going to be written");
      }
    }
  }

  @Test
  public void testWriteBehindQueueSize() throws Exception {

    class TestWriteBehindProvider extends WriteBehindProviderFactory.Provider {

      private WriteBehind<?, ?> writeBehind = null;

      @Override
      @SuppressWarnings("unchecked")
      public <K, V> WriteBehind<K, V> createWriteBehindLoaderWriter(CacheLoaderWriter<K, V> cacheLoaderWriter, WriteBehindConfiguration<?> configuration) {
        this.writeBehind = super.createWriteBehindLoaderWriter(cacheLoaderWriter, configuration);
        return (WriteBehind<K, V>) writeBehind;
      }

      public WriteBehind<?, ?> getWriteBehind() {
        return writeBehind;
      }
    }

    TestWriteBehindProvider writeBehindProvider = new TestWriteBehindProvider();
    WriteBehindTestLoaderWriter<String, String> loaderWriter = new WriteBehindTestLoaderWriter<>();

    try (CacheManager cacheManager = managerBuilder().using(writeBehindProvider).build(true)) {
      Cache<String, String> testCache = cacheManager.createCache("testAgedBatchedIsWritten", configurationBuilder()
        .withService(new DefaultCacheLoaderWriterConfiguration(loaderWriter))
        .withService(newBatchedWriteBehindConfiguration(5, SECONDS, 2).build())
        .build());

      testCache.put("key1", "value1");
      assertThat(writeBehindProvider.getWriteBehind().getQueueSize(), is(1L));
      testCache.put("key2", "value2");

    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  protected CacheLoaderWriterProvider getMockedCacheLoaderWriterProvider(CacheLoaderWriter<String, String> loaderWriter) {
    CacheLoaderWriterProvider cacheLoaderWriterProvider = mock(CacheLoaderWriterProvider.class);
    when(cacheLoaderWriterProvider.createCacheLoaderWriter(anyString(), (CacheConfiguration<String, String>)any())).thenReturn((CacheLoaderWriter) loaderWriter);
    return cacheLoaderWriterProvider;
  }

}
