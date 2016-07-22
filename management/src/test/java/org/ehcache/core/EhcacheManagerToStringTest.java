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

import org.ehcache.CacheManager;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.equalTo;

public class EhcacheManagerToStringTest {

  private CacheManager cacheManager;

  @Test
  public void simpleOnHeapToString() throws Exception {
    File root = new File("build/tmp/EhcacheManagerToStringTest");
    root.mkdirs();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(root))
        .withCache("cache-1", CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .disk(2, MemoryUnit.MB, true))
            .withLoaderWriter(new SampleLoaderWriter<String, String>())
            .add(WriteBehindConfigurationBuilder
                .newBatchedWriteBehindConfiguration(1, TimeUnit.SECONDS, 3)
                .queueSize(3)
                .concurrencyLevel(1)
                .enableCoalescing())
            .withEvictionAdvisor(new EvictionAdvisor<String, String>() {
              @Override
              public boolean adviseAgainstEviction(String key, String value) {
                return false;
              }
            })
            .build())
        .build(true);

    String actual = ((HumanReadable) cacheManager.getRuntimeConfiguration()).readableString();
    String expected = read("/simpleConfiguration.txt");

    // only testing part of the string, to avoid collections ordering clashes
    Assert.assertThat(
        actual.substring(actual.indexOf("resourcePools")).replace(" ", "").replace("\n", ""),
        equalTo(
            expected.substring(expected.indexOf("resourcePools")).replace(" ", "").replace("\n", "")
        )
    );
  }

  public static class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

    @Override
    public V load(K key) throws Exception {
      throw new UnsupportedOperationException("Implement Me");
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void write(K key, V value) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void delete(K key) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void deleteAll(Iterable<? extends K> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

  private String read(String path) throws FileNotFoundException {
    Scanner scanner = new Scanner(getClass().getResourceAsStream(path), "UTF-8");
    try {
      return scanner.useDelimiter("\\A").next();
    } finally {
      scanner.close();
    }
  }

}
