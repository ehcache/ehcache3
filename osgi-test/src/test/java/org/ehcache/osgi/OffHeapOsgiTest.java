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

package org.ehcache.osgi;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import java.io.Serializable;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * OffHeapOsgiTest
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class OffHeapOsgiTest {

  @Configuration
  public Option[] config() {
    return options(
        mavenBundle("org.slf4j", "slf4j-api", "1.7.7"),
        mavenBundle("org.slf4j", "slf4j-simple", "1.7.7").noStart(),
        mavenBundle("org.ehcache", "ehcache", "3.0.0-SNAPSHOT"),
        mavenBundle("org.terracotta", "offheap-store", "2.1.1"),
        junitBundles()
    );
  }

  @Test
  public void testOffHeapInOsgi() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder().withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
            .buildConfig(Long.class, String.class))
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

    cache.put(42L, "I am out of heap!!");

    cache.get(42L);
  }

  @Test
  public void testOffHeapClientClass() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withClassLoader(getClass().getClassLoader())
        .withCache("myCache", newCacheConfigurationBuilder().withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(2, MemoryUnit.MB))
            .buildConfig(Long.class, Order.class))
        .build(true);

    Cache<Long, Order> cache = cacheManager.getCache("myCache", Long.class, Order.class);

    Order order = new Order(42L);
    cache.put(42L, order);

    assertTrue(cache.get(42L) instanceof Order);

    cache.replace(42L, order, new Order(-1L));

    assertEquals(-1L, cache.get(42L).id);
  }

  private static class Order implements Serializable {
    final long id;

    Order(long id) {
      this.id = id;
    }

    @Override
    public int hashCode() {
      return (int) id;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof Order) {
        return ((Order) obj).id == this.id;
      }
      return false;
    }
  }
}
