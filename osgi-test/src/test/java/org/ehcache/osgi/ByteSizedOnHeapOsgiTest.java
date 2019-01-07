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

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.osgi.OsgiTestUtils.baseConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.gradleBundle;
import static org.ehcache.osgi.OsgiTestUtils.wrappedGradleBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class ByteSizedOnHeapOsgiTest {

  @Configuration
  public Option[] individualModules() {
    return options(
      gradleBundle("org.ehcache.modules:impl"),
      gradleBundle("org.ehcache.modules:core"),
      gradleBundle("org.ehcache.modules:api"),

      wrappedGradleBundle("org.terracotta:statistics"),
      wrappedGradleBundle("org.ehcache:sizeof"),
      wrappedGradleBundle("org.terracotta:offheap-store"),

      baseConfiguration("ByteSizedOnHeapOsgiTest", "individualModules")
    );
  }

  @Configuration
  public Option[] uberJar() {
    return options(
      gradleBundle("org.ehcache:dist"),

      baseConfiguration("ByteSizedOnHeapOsgiTest", "uberJar")
    );
  }

  @Test
  public void testByteSizedOnHeapInOsgi() {
    TestMethods.testByteSizedOnHeapInOsgi();
  }

  private static class TestMethods {
    public static void testByteSizedOnHeapInOsgi() {
      CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, String.class, newResourcePoolsBuilder().heap(10, MemoryUnit.KB))
          .build())
        .build(true);

      Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

      cache.put(42L, "I am out of heap!!");

      cache.get(42L);
    }
  }
}
