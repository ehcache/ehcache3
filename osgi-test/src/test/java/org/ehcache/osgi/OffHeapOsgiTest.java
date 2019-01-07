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
import org.ehcache.config.builders.CacheManagerBuilder;
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

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.osgi.EhcacheActivator.OSGI_LOADING;
import static org.ehcache.osgi.OsgiTestUtils.baseConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.gradleBundle;
import static org.ehcache.osgi.OsgiTestUtils.wrappedGradleBundle;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.frameworkProperty;
import static org.ops4j.pax.exam.CoreOptions.options;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class OffHeapOsgiTest {

  @Configuration
  public Option[] individualModules() {
    return options(
      gradleBundle("org.ehcache.modules:api"),
      gradleBundle("org.ehcache.modules:core"),
      gradleBundle("org.ehcache.modules:impl"),

      wrappedGradleBundle("org.terracotta:statistics"),
      wrappedGradleBundle("org.ehcache:sizeof"),
      wrappedGradleBundle("org.terracotta:offheap-store"),

      baseConfiguration("OffHeapOsgiTest", "individualModules")
    );
  }

  @Configuration
  public Option[] uberJarWithOsgiServiceLoading() {
    return options(
      gradleBundle("org.ehcache:dist"),

      baseConfiguration("OffHeapOsgiTest", "uberJarWithOsgiServiceLoading")
    );
  }

  @Configuration
  public Option[] uberJarWithJdkServiceLoading() {
    return options(
      frameworkProperty(OSGI_LOADING).value("false"),

      gradleBundle("org.ehcache:dist"),

      baseConfiguration("OffHeapOsgiTest", "uberJarWithJdkServiceLoading")
    );
  }

  @Test
  public void testOffHeapInOsgi() {
    TestMethods.testOffHeapInOsgi();
  }

  @Test
  public void testOffHeapClientClass() {
    TestMethods.testOffHeapClientClass();
  }

  private static class TestMethods {

    public static void testOffHeapInOsgi() {
      CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(10, MemoryUnit.MB))
          .build())
        .build(true);

      Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

      cache.put(42L, "I am out of heap!!");

      cache.get(42L);
    }

    public static void testOffHeapClientClass() {
      CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withClassLoader(TestMethods.class.getClassLoader())
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, Order.class, newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).offheap(2, MemoryUnit.MB))
          .build())
        .build(true);

      Cache<Long, Order> cache = cacheManager.getCache("myCache", Long.class, Order.class);

      Order order = new Order(42L);
      cache.put(42L, order);

      assertTrue(cache.get(42L) instanceof Order);

      cache.replace(42L, order, new Order(-1L));

      assertEquals(-1L, cache.get(42L).id);
    }

    private static class Order implements Serializable {

      private static final long serialVersionUID = 1L;

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
}
