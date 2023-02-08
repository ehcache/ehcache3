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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration;
import static org.ehcache.config.builders.WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.terracotta.utilities.test.matchers.Eventually.within;

public class WriteBehindFailuresTest {

  private static final String keyValueStr = "Value-";

  @Test
  public void testNonBatchingWriteBehindFailure() throws Exception {
    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
      Cache<Integer, String> nonBatchingWriteBehindCache = cacheManager.createCache("nonBatchingWriteBehindCache",
        newCacheConfigurationBuilder(Integer.class, String.class, ResourcePoolsBuilder.heap(100))
          .withLoaderWriter(new EvenNumberLoaderWriter())
          .withService(newUnBatchedWriteBehindConfiguration()));

      for (int i = 1; i <= 10; i++)
        nonBatchingWriteBehindCache.put(i, String.valueOf(keyValueStr + i));

      for (int i = 1; i <= 10; i++) {
        if ((i % 2) != 0) {
          final int key = i;
          assertThat(() -> nonBatchingWriteBehindCache.get(key), within(Duration.ofSeconds(10)).matches(is(nullValue())));
        } else
          assertThat(nonBatchingWriteBehindCache.get(i), is(String.valueOf(keyValueStr + i)));
      }
    }
  }

  @Test
  public void testNonBatchingWriteBehindFailureImmediateGetAfterPut() throws Exception {
    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
      Cache<Integer, String> nonBatchingWriteBehindCache = cacheManager.createCache("nonBatchingWriteBehindCache",
        newCacheConfigurationBuilder(Integer.class, String.class, ResourcePoolsBuilder.heap(100))
          .withLoaderWriter(new EvenNumberLoaderWriter())
          .withService(newUnBatchedWriteBehindConfiguration()));

      for (int i = 1; i <= 10; i++)
        nonBatchingWriteBehindCache.put(i, String.valueOf(keyValueStr + i));

      for (int i = 1; i <= 10; i++) {
        if ((i % 2) != 0) {
          final int key = i;
          String val = nonBatchingWriteBehindCache.get(key);
          assertThat(() -> nonBatchingWriteBehindCache.get(key), within(Duration.ofSeconds(10)).matches(is(nullValue())));
        } else
          assertThat(nonBatchingWriteBehindCache.get(i), is(String.valueOf(keyValueStr + i)));
      }
    }
  }

  @Test
  public void testBatchingWriteBehindFailure() {
    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
      Cache<Integer, String> batchingWriteBehindCache = cacheManager.createCache("batchingWriteBehindCache",
        newCacheConfigurationBuilder(Integer.class, String.class, ResourcePoolsBuilder.heap(100))
          .withLoaderWriter(new EvenNumberLoaderWriter())
          .withService(newBatchedWriteBehindConfiguration(2, TimeUnit.SECONDS, 100)));

      for (int i = 1; i <= 10; i++) {
        batchingWriteBehindCache.put(i, String.valueOf(keyValueStr + i));
      }

      for (int i = 1; i <= 10; i++) {
        final int key = i;
        assertThat(() -> batchingWriteBehindCache.get(key), within(Duration.ofSeconds(10)).matches(is(nullValue())));
      }
    }
  }

  @Test
  public void testBatchingWriteBehindFailureImmediateGetAfterPut() {
    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
      Cache<Integer, String> batchingWriteBehindCache1 = cacheManager.createCache("batchingWriteBehindCache",
        newCacheConfigurationBuilder(Integer.class, String.class, ResourcePoolsBuilder.heap(100))
          .withLoaderWriter(new EvenNumberLoaderWriter())
          .withService(newBatchedWriteBehindConfiguration(2, TimeUnit.SECONDS, 100)));

      for (int i = 1; i <= 10; i++) {
        batchingWriteBehindCache1.put(i, String.valueOf(keyValueStr + i));
      }

      for (int i = 1; i <= 10; i++) {
        final int key = i;
        String val = batchingWriteBehindCache1.get(key);
        assertThat(() -> batchingWriteBehindCache1.get(key), within(Duration.ofSeconds(10)).matches(is(nullValue())));
      }
    }
  }

  @Test
  public void testBatchingWriteBehindFailureForWriteAll() {
    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder().build(true)) {
      Cache<Integer, String> cache = cacheManager.createCache("batchingWriteBehindWriteAll",
        newCacheConfigurationBuilder(Integer.class, String.class, ResourcePoolsBuilder.heap(100))
          .withLoaderWriter(new OddNumberLoaderWriter())
          .withService(newBatchedWriteBehindConfiguration(2, TimeUnit.SECONDS, 100)));

      Map<Integer, String> dataMap = new HashMap<>();
      for (int i = 1; i <= 10; i++) {
        dataMap.put(i, String.valueOf(i));
      }

      cache.putAll(dataMap);

      for (int i = 1; i <= 10; i++) {
        final int key = i;
        assertThat(() -> cache.get(key), within(Duration.ofSeconds(10)).matches(is(nullValue())));
      }
    }
  }

  private static class EvenNumberLoaderWriter implements CacheLoaderWriter<Integer, String> {

    @Override
    public String load(Integer key) {
      return null;
    }

    @Override
    public void write(Integer key, String value) throws Exception {
      if ((key % 2) != 0) {
        throw new RuntimeException("Only even keys can be added");
      }
    }

    @Override
    public void delete(Integer key) throws Exception {
    }

  }

  private static class OddNumberLoaderWriter implements CacheLoaderWriter<Integer, String> {

    @Override
    public String load(Integer key) {
      return null;
    }

    @Override
    public void write(Integer key, String value) {
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends Integer, ? extends String>> entries) throws Exception {
      Map<Integer, String> dataMap = new HashMap<>();
      for (Map.Entry<? extends Integer, ? extends String> entry : entries) {
        Integer key = entry.getKey();
        if ((key % 2) == 0)
          throw new Exception("Only odd keys can be added");
      }
    }

    @Override
    public void delete(Integer key) throws Exception {
    }
  }

}
