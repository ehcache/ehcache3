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
import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.config.copy.DefaultCopierConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.internal.copy.ReadWriteCopier;
import org.ehcache.internal.copy.SerializingCopier;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;

import static org.ehcache.config.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.junitBundles;
import static org.ops4j.pax.exam.CoreOptions.mavenBundle;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * SimpleOsgiTest
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class SimpleOsgiTest {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Configuration
  public Option[] config() {
    return options(
        mavenBundle("org.slf4j", "slf4j-api", "1.7.7"),
        mavenBundle("org.slf4j", "slf4j-simple", "1.7.7").noStart(),
        mavenBundle("org.ehcache", "ehcache", "3.0.0-SNAPSHOT"),
        junitBundles()
    );
  }

  @Test
  public void testEhcache3AsBundle() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder().withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
            .buildConfig(Long.class, String.class))
        .build(true);

    Cache<Long, String> myCache = cacheManager.getCache("myCache", Long.class, String.class);

    myCache.put(42L, "DaAnswer!");
    assertEquals("DaAnswer!", myCache.get(42L));
  }

  @Test
  public void testEhcache3WithSerializationAndClientClass() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder().withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
            .add(new DefaultCopierConfiguration<Person>((Class) SerializingCopier.class, CopierConfiguration.Type.VALUE))
            .withClassLoader(getClass().getClassLoader())
            .buildConfig(Long.class, Person.class))
        .build(true);

    Cache<Long, Person> myCache = cacheManager.getCache("myCache", Long.class, Person.class);

    myCache.put(42L, new Person("Arthur"));
    assertTrue(myCache.get(42L) instanceof Person);
  }

  @Test
  public void testCustomCopier() {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder().withResourcePools(newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES))
            .add(new DefaultCopierConfiguration<String>(StringCopier.class, CopierConfiguration.Type.VALUE))
            .withClassLoader(getClass().getClassLoader())
            .buildConfig(Long.class, String.class))
        .build(true);

    Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

    cache.put(42L, "What's the question again?");
    cache.get(42L);
  }

  @Test
  public void testEhcacheXMLConfig() throws Exception {
    File config = writeEhcacheConfig();

    new XmlConfiguration(config.toURI().toURL(), getClass().getClassLoader());
  }

  private File writeEhcacheConfig() throws IOException {
    File config = temporaryFolder.newFile("ehcache-config.xml");

    PrintWriter writer = new PrintWriter(config);
    writer.println("<ehcache:config\n" +
                   "  xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n" +
                   "  xmlns:foo='http://www.example.com/foo'\n" +
                   "  xmlns:ehcache='http://www.ehcache.org/v3'\n" +
                   "  xsi:schemaLocation=\"http://www.ehcache.org/v3 ../../../../xml/src/main/resources/ehcache-core.xsd\">\n" +
                   "\n" +
                   "  <ehcache:cache alias=\"bar\">\n" +
                   "    <ehcache:key-type>java.lang.String</ehcache:key-type>\n" +
                   "    <ehcache:value-type>java.lang.String</ehcache:value-type>\n" +
                   "  </ehcache:cache>\n" +
                   "</ehcache:config>");
    writer.close();
    return config;
  }

  public static class StringCopier extends ReadWriteCopier<String> {

    @Override
    public String copy(String obj) {
      return new String(obj);
    }
  }

  private static class Person implements Serializable {

    final String name;
    Person(String name) {
      this.name = name;
    }

  }
}
