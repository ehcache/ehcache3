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
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.copy.ReadWriteCopier;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.bundle;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * SimpleOsgiTest
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class SimpleOsgiTest {

  @Configuration
  public Option[] config() {
    return options(
        mavenBundle("org.slf4j", "slf4j-api", System.getProperty("ehcache.osgi.slf4j.version")),
        mavenBundle("org.slf4j", "slf4j-simple", System.getProperty("ehcache.osgi.slf4j.version")).noStart(),
        bundle("file:" + System.getProperty("ehcache.osgi.jar")),
        junitBundles()
    );
  }

  @Test
  public void testEhcache3AsBundle() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, String.class, heap(10))
            .build())
        .build(true);

    Cache<Long, String> myCache = cacheManager.getCache("myCache", Long.class, String.class);

    myCache.put(42L, "DaAnswer!");
    assertEquals("DaAnswer!", myCache.get(42L));
  }

  @Test
  public void testEhcache3WithSerializationAndClientClass() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, Person.class, heap(10))
            .add(new DefaultCopierConfiguration<>(SerializingCopier.<Person>asCopierClass(), DefaultCopierConfiguration.Type.VALUE))
            .withClassLoader(getClass().getClassLoader())
            .build())
        .build(true);

    Cache<Long, Person> myCache = cacheManager.getCache("myCache", Long.class, Person.class);

    myCache.put(42L, new Person("Arthur"));
    assertTrue(myCache.get(42L) instanceof Person);
  }

  @Test
  public void testCustomCopier() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, String.class, heap(10))
            .add(new DefaultCopierConfiguration<>(StringCopier.class, DefaultCopierConfiguration.Type.VALUE))
            .withClassLoader(getClass().getClassLoader())
            .build())
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

    cache.put(42L, "What's the question again?");
    cache.get(42L);
  }

  @Test
  public void testEhcacheXMLConfig() throws Exception {
    XmlConfiguration configuration = new XmlConfiguration(getClass().getResource("/org/ehcache/osgi/ehcache-osgi.xml").toURI().toURL(), getClass().getClassLoader());

    assertEquals(Person.class, configuration.getCacheConfigurations().get("bar").getValueType());
  }

  public static class StringCopier extends ReadWriteCopier<String> {

    @Override
    public String copy(String obj) {
      return new String(obj);
    }
  }

}
