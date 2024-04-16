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

package org.ehcache.config.builders;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.CacheConfiguration;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.config.units.MemoryUnit.MB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import org.terracotta.org.junit.rules.TemporaryFolder;

public class CacheManagerBuilderSharedResourcesTest {
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  private final Long LONG1 = 11L;
  private final Long LONG2 = 22L;
  private final Double DOUBLE1 = 11.0;
  private final Double DOUBLE2 = 22.0;
  private final String STRING1 = "11";
  private final String STRING2 = "22";

  @Test
  public void testSharedOffHeapMultiType() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedOffheap()))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedOffheap()))
      .withCache("c3", cacheConfig(String.class, String.class, newResourcePoolsBuilder().sharedOffheap()))
      .withCache("c4", cacheConfig(String.class, String.class, newResourcePoolsBuilder().sharedOffheap()))
      .withCache("c5", cacheConfig(Double.class, Double.class, newResourcePoolsBuilder().sharedOffheap()))
      .withCache("c6", cacheConfig(Double.class, Double.class, newResourcePoolsBuilder().sharedOffheap()))
      .build(true);
    Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
    Cache<Long, Long> c2 = cacheManager.getCache("c2", Long.class, Long.class);
    Cache<String, String> c3 = cacheManager.getCache("c3", String.class, String.class);
    Cache<String, String> c4 = cacheManager.getCache("c4", String.class, String.class);
    Cache<Double, Double> c5 = cacheManager.getCache("c5", Double.class, Double.class);
    Cache<Double, Double> c6 = cacheManager.getCache("c6", Double.class, Double.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    exerciseCacheApi(c3, c4, STRING1, STRING2);
    exerciseCacheApi(c5, c6, DOUBLE1, DOUBLE2);
    cacheManager.close();
  }

  @Test
  public void testSharedOffHeap() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().offheap(10, MB)))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedOffheap()))
      .build(true);
    Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
    Cache<Long, Long> c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedOffheap()))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().offheap(10, MB)))
      .build(true);
    c1 = cacheManager.getCache("c1", Long.class, Long.class);
    c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedOffheap()))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedOffheap()))
      .build(true);
    c1 = cacheManager.getCache("c1", Long.class, Long.class);
    c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();
  }

  @Test
  public void testTwoTierSharedOffHeap() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).offheap(20, MB)))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).sharedOffheap()))
      .build(true);
    Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
    Cache<Long, Long> c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).sharedOffheap()))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).offheap(20, MB)))
      .build(true);
    c1 = cacheManager.getCache("c1", Long.class, Long.class);
    c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).sharedOffheap()))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).sharedOffheap()))
      .build(true);
    c1 = cacheManager.getCache("c1", Long.class, Long.class);
    c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();
  }

  @Test
  public void testThreeTierSharedOffHeap() throws IOException {
    PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .with(CacheManagerBuilder.persistence((new File(temporaryFolder.newFolder().getAbsolutePath()))))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).offheap(20, MB).disk(30, MB)))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).sharedOffheap().disk(30, MB)))
      .build(true);
    Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
    Cache<Long, Long> c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .with(CacheManagerBuilder.persistence((new File(temporaryFolder.newFolder().getAbsolutePath()))))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).sharedOffheap().disk(30, MB)))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).offheap(20, MB).disk(30, MB)))
      .build(true);
    c1 = cacheManager.getCache("c1", Long.class, Long.class);
    c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .sharedResources(newResourcePoolsBuilder().offheap(60, MB))
      .with(CacheManagerBuilder.persistence((new File(temporaryFolder.newFolder().getAbsolutePath()))))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).sharedOffheap().disk(30, MB)))
      .withCache("c2", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(10, MB).sharedOffheap().disk(30, MB)))
      .build(true);
    c1 = cacheManager.getCache("c1", Long.class, Long.class);
    c2 = cacheManager.getCache("c2", Long.class, Long.class);
    exerciseCacheApi(c1, c2, LONG1, LONG2);
    cacheManager.close();
  }

  @Test
  public void testSharedDisk() throws IOException {
    long heapSize = 30;
    long offHeapSize = 80;
    long diskSize = 90;
    File folder = new File(temporaryFolder.newFolder().getAbsolutePath());
    //File folder = new File("C:\\temp\\test");
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c2", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c3", cacheConfig(String.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c4", cacheConfig(String.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c5", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c6", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c7", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c8", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .withCache("c9", cacheConfig(String.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .build(true)) {

      Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
      Cache<Long, String> c2 = cacheManager.getCache("c2", Long.class, String.class);
      Cache<String, Long> c3 = cacheManager.getCache("c3", String.class, Long.class);
      Cache<String, String> c4 = cacheManager.getCache("c4", String.class, String.class);
      Cache<Long, Long> c5 = cacheManager.getCache("c5", Long.class, Long.class);
      Cache<Long, Long> c6 = cacheManager.getCache("c6", Long.class, Long.class);
      Cache<Long, String> c7 = cacheManager.getCache("c7", Long.class, String.class);
      Cache<Long, Long> c8 = cacheManager.getCache("c8", Long.class, Long.class);
      Cache<String, String> c9 = cacheManager.getCache("c9", String.class, String.class);

      c1.put(1L, 11L);
      c2.put(2L, "22");
      c3.put("3", 33L);
      c4.put("4", "44");
      c5.put(5L, 55L);
      c6.put(6L, 66L);
      c7.put(7L, "77");
      c8.put(8L, 88L);
      c9.put("9", "99");
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c9", cacheConfig(String.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .build(true)) {
      Cache<String, String> c9 = cacheManager.getCache("c9", String.class, String.class);
      assertThat(c9.get("9"), is("99"));
      c9.put("99", "999");
    }
    long key = 87;
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c8", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .build(true)) {
      Cache<Long, Long> c8 = cacheManager.getCache("c8", Long.class, Long.class);
      assertThat(c8.get(8L), is(88L));
      c8.put(key, 888L);
      assertThat(c8.get(key), is(888L));
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c7", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .build(true)) {
      Cache<Long, String> c7 = cacheManager.getCache("c7", Long.class, String.class);
      assertThat(c7.get(7L), is("77"));
      c7.put(77L, "777");
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c6", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .build(true)) {
      Cache<Long, Long> c6 = cacheManager.getCache("c6", Long.class, Long.class);
      assertThat(c6.get(6L), is(66L));
      c6.put(66L, 666L);
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c5", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .build(true)) {
      Cache<Long, Long> c5 = cacheManager.getCache("c5", Long.class, Long.class);
      assertThat(c5.get(5L), is(55L));
      c5.put(55L, 555L);
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c4", cacheConfig(String.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .build(true)) {
      Cache<String, String> c4 = cacheManager.getCache("c4", String.class, String.class);
      assertThat(c4.get("4"), is("44"));
      c4.put("44", "444");
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c3", cacheConfig(String.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .build(true)) {
      Cache<String, Long> c3 = cacheManager.getCache("c3", String.class, Long.class);
      assertThat(c3.get("3"), is(33L));
      c3.put("33", 333L);
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c2", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .build(true)) {
      Cache<Long, String> c2 = cacheManager.getCache("c2", Long.class, String.class);
      assertThat(c2.get(2L), is("22"));
      c2.put(22L, "222");
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .build(true)) {
      Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
      assertThat(c1.get(1L), is(11L));
      c1.put(11L, 111L);
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c2", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c3", cacheConfig(String.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c4", cacheConfig(String.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c5", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c6", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c7", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c8", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .withCache("c9", cacheConfig(String.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .build(true)) {

      Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
      Cache<Long, String> c2 = cacheManager.getCache("c2", Long.class, String.class);
      Cache<String, Long> c3 = cacheManager.getCache("c3", String.class, Long.class);
      Cache<String, String> c4 = cacheManager.getCache("c4", String.class, String.class);
      Cache<Long, Long> c5 = cacheManager.getCache("c5", Long.class, Long.class);
      Cache<Long, Long> c6 = cacheManager.getCache("c6", Long.class, Long.class);
      Cache<Long, String> c7 = cacheManager.getCache("c7", Long.class, String.class);
      Cache<Long, Long> c8 = cacheManager.getCache("c8", Long.class, Long.class);
      Cache<String, String> c9 = cacheManager.getCache("c9", String.class, String.class);

      assertThat(c1.get(1L), is(11L));
      assertThat(c2.get(2L), is("22"));
      assertThat(c3.get("3"), is(33L));
      assertThat(c4.get("4"), is("44"));
      assertThat(c5.get(5L), is(55L));
      assertThat(c6.get(6L), is(66L));
      assertThat(c7.get(7L), is("77"));
      assertThat(c8.get(8L), is(88L));
      assertThat(c9.get("9"), is("99"));

      assertThat(c1.get(11L), is(111L));
      assertThat(c2.get(22L), is("222"));
      assertThat(c3.get("33"), is(333L));
      assertThat(c4.get("44"), is("444"));
      assertThat(c5.get(55L), is(555L));
      assertThat(c6.get(66L), is(666L));
      assertThat(c7.get(77L), is("777"));
      assertThat(c8.get(key), is(888L));
      assertThat(c9.get("99"), is("999"));
    }
    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c9", cacheConfig(String.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .withCache("c8", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .withCache("c7", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c6", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c5", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c4", cacheConfig(String.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c3", cacheConfig(String.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c2", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .build(true)) {

      Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
      Cache<Long, String> c2 = cacheManager.getCache("c2", Long.class, String.class);
      Cache<String, Long> c3 = cacheManager.getCache("c3", String.class, Long.class);
      Cache<String, String> c4 = cacheManager.getCache("c4", String.class, String.class);
      Cache<Long, Long> c5 = cacheManager.getCache("c5", Long.class, Long.class);
      Cache<Long, Long> c6 = cacheManager.getCache("c6", Long.class, Long.class);
      Cache<Long, String> c7 = cacheManager.getCache("c7", Long.class, String.class);
      Cache<Long, Long> c8 = cacheManager.getCache("c8", Long.class, Long.class);
      Cache<String, String> c9 = cacheManager.getCache("c9", String.class, String.class);

      assertThat(c1.get(1L), is(11L));
      assertThat(c2.get(2L), is("22"));
      assertThat(c3.get("3"), is(33L));
      assertThat(c4.get("4"), is("44"));
      assertThat(c5.get(5L), is(55L));
      assertThat(c6.get(6L), is(66L));
      assertThat(c7.get(7L), is("77"));
      assertThat(c8.get(8L), is(88L));
      assertThat(c9.get("9"), is("99"));

      assertThat(c1.get(11L), is(111L));
      assertThat(c2.get(22L), is("222"));
      assertThat(c3.get("33"), is(333L));
      assertThat(c4.get("44"), is("444"));
      assertThat(c5.get(55L), is(555L));
      assertThat(c6.get(66L), is(666L));
      assertThat(c7.get(77L), is("777"));
      assertThat(c8.get(key), is(888L));
      assertThat(c9.get("99"), is("999"));
    }

    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c10", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c11", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c12", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .withCache("c13", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().disk(diskSize, MB, true)))
      .withCache("c14", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).disk(diskSize, MB, true)))
      .withCache("c15", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().disk(diskSize, MB, true)))
      .build(true)) {

      Cache<Long, Long> c10 = cacheManager.getCache("c10", Long.class, Long.class);
      Cache<Long, Long> c11 = cacheManager.getCache("c11", Long.class, Long.class);
      Cache<Long, Long> c12 = cacheManager.getCache("c12", Long.class, Long.class);
      Cache<Long, Long> c13 = cacheManager.getCache("c13", Long.class, Long.class);
      Cache<Long, Long> c14 = cacheManager.getCache("c14", Long.class, Long.class);
      Cache<Long, Long> c15 = cacheManager.getCache("c15", Long.class, Long.class);

      c10.put(10L, 10L);
      c11.put(11L, 11L);
      c12.put(12L, 12L);
      c13.put(13L, 13L);
      c14.put(14L, 14L);
      c15.put(15L, 15L);
    }

    try (PersistentCacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence((folder)))
      .sharedResources(newResourcePoolsBuilder().offheap(offHeapSize, MB).disk(diskSize, MB, true))
      .withCache("c15", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().disk(diskSize, MB, true)))
      .withCache("c14", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).disk(diskSize, MB, true)))
      .withCache("c13", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().disk(diskSize, MB, true)))
      .withCache("c12", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .withCache("c11", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c10", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c9", cacheConfig(String.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .withCache("c8", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedOffheap().sharedDisk()))
      .withCache("c7", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c6", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().heap(heapSize, MB).sharedDisk()))
      .withCache("c5", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c4", cacheConfig(String.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c3", cacheConfig(String.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c2", cacheConfig(Long.class, String.class, newResourcePoolsBuilder().sharedDisk()))
      .withCache("c1", cacheConfig(Long.class, Long.class, newResourcePoolsBuilder().sharedDisk()))
      .build(true)) {

      Cache<Long, Long> c1 = cacheManager.getCache("c1", Long.class, Long.class);
      Cache<Long, String> c2 = cacheManager.getCache("c2", Long.class, String.class);
      Cache<String, Long> c3 = cacheManager.getCache("c3", String.class, Long.class);
      Cache<String, String> c4 = cacheManager.getCache("c4", String.class, String.class);
      Cache<Long, Long> c5 = cacheManager.getCache("c5", Long.class, Long.class);
      Cache<Long, Long> c6 = cacheManager.getCache("c6", Long.class, Long.class);
      Cache<Long, String> c7 = cacheManager.getCache("c7", Long.class, String.class);
      Cache<Long, Long> c8 = cacheManager.getCache("c8", Long.class, Long.class);
      Cache<String, String> c9 = cacheManager.getCache("c9", String.class, String.class);
      Cache<Long, Long> c10 = cacheManager.getCache("c10", Long.class, Long.class);
      Cache<Long, Long> c11 = cacheManager.getCache("c11", Long.class, Long.class);
      Cache<Long, Long> c12 = cacheManager.getCache("c12", Long.class, Long.class);
      Cache<Long, Long> c13 = cacheManager.getCache("c13", Long.class, Long.class);
      Cache<Long, Long> c14 = cacheManager.getCache("c14", Long.class, Long.class);
      Cache<Long, Long> c15 = cacheManager.getCache("c15", Long.class, Long.class);

      assertThat(c1.get(1L), is(11L));
      assertThat(c2.get(2L), is("22"));
      assertThat(c3.get("3"), is(33L));
      assertThat(c4.get("4"), is("44"));
      assertThat(c5.get(5L), is(55L));
      assertThat(c6.get(6L), is(66L));
      assertThat(c7.get(7L), is("77"));
      assertThat(c8.get(8L), is(88L));
      assertThat(c9.get("9"), is("99"));

      assertThat(c1.get(11L), is(111L));
      assertThat(c2.get(22L), is("222"));
      assertThat(c3.get("33"), is(333L));
      assertThat(c4.get("44"), is("444"));
      assertThat(c5.get(55L), is(555L));
      assertThat(c6.get(66L), is(666L));
      assertThat(c7.get(77L), is("777"));
      assertThat(c8.get(key), is(888L));
      assertThat(c9.get("99"), is("999"));

      assertThat(c10.get(10L), is(10L));
      assertThat(c11.get(11L), is(11L));
      assertThat(c12.get(12L), is(12L));
      assertThat(c13.get(13L), is(13L));
      assertThat(c14.get(14L), is(14L));
      assertThat(c15.get(15L), is(15L));
    }
  }

  private <K, V> CacheConfiguration<K, V> cacheConfig(Class<K> keyType, Class<V> valueType, ResourcePoolsBuilder poolsBuilder) {
    return CacheConfigurationBuilder.newCacheConfigurationBuilder(keyType, valueType, poolsBuilder).build();
  }

  @SuppressWarnings("unchecked")
  private <K,V> void exerciseCacheApi(Cache<K,V> c1, Cache<K,V> c2, K key1, K key2) {
    V value1 = (V) key1;
    V value2 = (V) key2;
    assertNull(c1.putIfAbsent(key1, value1));
    assertThat(c1.containsKey(key1), is(true));
    assertThat(c1.get(key1), is(value1));
    assertThat(c1.putIfAbsent(key1, value2), is(value1));
    assertThat(c1.get(key1), is(value1));
    c1.remove(key1);
    c1.put(key1, value1);
    assertThat(c1.containsKey(key1), is(true));
    assertThat(c2.containsKey(key1), is(false));
    c2.put(key2, value2);
    assertThat(c1.containsKey(key2), is(false));
    assertThat(c2.containsKey(key2), is(true));
    c2.put(key2, value2);
    assertThat(c1.containsKey(key2), is(false));
    assertThat(c2.containsKey(key2), is(true));
    assertThat(c1.get(key1), is(value1));
    assertThat(c2.get(key2), is(value2));
    V oldValue = c1.replace(key1, value2);
    assertThat(oldValue, is(value1));
    assertThat(c1.get(key1), is(value2));
    assertThat(c2.get(key2), is(value2));
    assertThat(c2.replace(key2, value1, value2), is(false));
    assertThat(c2.replace(key2, value2, value1), is(true));
    assertThat(c2.get(key2), is(value1));
    assertThat(c2.replace(key2, value1, value2), is(true));
    assertThat(c2.get(key2), is(value2));
    c1.put(key2, value1);
    c2.put(key1, value2);
    assertThat(c1.get(key2), is(value1));
    assertThat(c2.get(key1), is(value2));
    c1.put(key1, value2);
    c2.put(key2, value1);
    c1.put(key2, value2);
    c2.put(key1, value1);
    assertThat(c1.get(key1), is(value2));
    assertThat(c2.get(key2), is(value1));
    assertThat(c1.get(key2), is(value2));
    assertThat(c2.get(key1), is(value1));
    assertThat(c1.remove(key1, value1), is(false));
    assertThat(c1.containsKey(key1), is(true));
    assertThat(c1.remove(key1, value2), is(true));
    assertThat(c1.containsKey(key1), is(false));
    c1.remove(key2);
    assertThat(c1.containsKey(key2), is(false));
    c2.remove(key1);
    assertThat(c2.containsKey(key1), is(false));
    assertThat(c2.containsKey(key2), is(true));

    // bulk ops
    int mapCount = 10;
    Map<? extends K, ? extends K> map = new HashMap<>();
    for (long i = 1; i <= mapCount; i++) {
      if (key1 instanceof Long) {
        ((Map) map).put(i, i);
      } else if (key1 instanceof Double) {
        ((Map) map).put(i * 1.0, i * 1.0);
      } else { // String
        ((Map) map).put(Long.toString(i), Long.toString(i));
      }
    }
    c1.putAll((Map<? extends K, ? extends V>) map);
    Map<K, V> longResults = c1.getAll(map.keySet());
    assertThat(longResults.size(), is(mapCount));
    longResults.forEach((k, v) -> assertThat(v, is(k)));
    longResults = c2.getAll(map.keySet());
    assertThat(longResults.size(), is(mapCount));
    longResults.forEach((k, v) -> assertNull(v));
    c1.removeAll(map.keySet());
    longResults.forEach((k, v) -> assertNull(v));
    c1.putAll((Map<? extends K, ? extends V>) map);
    longResults = c1.getAll(map.keySet());
    assertThat(longResults.size(), is(mapCount));
    longResults.forEach((k, v) -> assertThat(v, is(k)));
    c1.removeAll(map.keySet());
    longResults = c1.getAll(map.keySet());
    assertThat(longResults.size(), is(mapCount));
    longResults.forEach((k, v) -> assertNull(v));
    c2.removeAll(map.keySet());
    c1.putAll((Map<? extends K, ? extends V>) map);
    c2.putAll((Map<? extends K, ? extends V>) map);
    longResults = c1.getAll(map.keySet());
    longResults.forEach((k, v) -> assertThat(v, is(k)));
    longResults = c2.getAll(map.keySet());
    longResults.forEach((k, v) -> assertThat(v, is(k)));
    c1.clear();
    longResults = c1.getAll(map.keySet());
    longResults.forEach((k, v) -> assertNull(v));
    longResults = c2.getAll(map.keySet());
    longResults.forEach((k, v) -> assertThat(v, is(k)));
  }
}
