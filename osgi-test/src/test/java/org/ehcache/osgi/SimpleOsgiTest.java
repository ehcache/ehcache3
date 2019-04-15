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
import org.ehcache.core.osgi.EhcacheActivator;
import org.ehcache.core.osgi.OsgiServiceLoader;
import org.ehcache.core.spi.service.ServiceFactory;
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
import org.osgi.framework.wiring.BundleWiring;

import java.util.ServiceLoader;
import java.util.Set;

import static java.util.Spliterators.spliterator;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static java.util.stream.StreamSupport.stream;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.osgi.EhcacheActivator.OSGI_LOADING;
import static org.ehcache.osgi.OsgiTestUtils.baseConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.gradleBundle;
import static org.ehcache.osgi.OsgiTestUtils.wrappedGradleBundle;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.ops4j.pax.exam.CoreOptions.frameworkProperty;
import static org.ops4j.pax.exam.CoreOptions.options;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class SimpleOsgiTest {

  @Configuration
  public Option[] individualModules() {
    return options(
      gradleBundle("org.ehcache.modules:api"),
      gradleBundle("org.ehcache.modules:core"),
      gradleBundle("org.ehcache.modules:impl"),
      gradleBundle("org.ehcache.modules:xml"),

      wrappedGradleBundle("org.terracotta:statistics"),
      wrappedGradleBundle("org.ehcache:sizeof"),
      wrappedGradleBundle("org.terracotta:offheap-store"),

      baseConfiguration("SimpleOsgiTest", "individualModules")
    );
  }

  @Configuration
  public Option[] uberJarWithOsgiServiceLoading() {
    return options(
      gradleBundle("org.ehcache:dist"),

      baseConfiguration("SimpleOsgiTest", "uberJarWithOsgiServiceLoading")
    );
  }

  @Configuration
  public Option[] uberJarWithJdkServiceLoading() {
    return options(
      frameworkProperty(OSGI_LOADING).value("false"),

      gradleBundle("org.ehcache:dist"),

      baseConfiguration("SimpleOsgiTest", "uberJarWithJdkServiceLoading")
    );
  }

  @Test
  public void testEhcache3AsBundle() {
    TestMethods.testEhcache3AsBundle();
  }

  @Test
  public void testEhcache3WithSerializationAndClientClass() {
    TestMethods.testEhcache3WithSerializationAndClientClass();
  }

  @Test
  public void testCustomCopier() {
    TestMethods.testCustomCopier();
  }

  @Test
  public void testEhcacheXMLConfig() throws Exception {
    TestMethods.testEhcacheXMLConfig();
  }

  @Test
  public void testAllServicesAreAvailable() {
    TestMethods.testAllServicesAreAvailable();
  }

  private static class TestMethods {

    public static void testEhcache3AsBundle() {
      CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, String.class, heap(10))
          .build())
        .build(true);

      Cache<Long, String> myCache = cacheManager.getCache("myCache", Long.class, String.class);

      myCache.put(42L, "DaAnswer!");
      assertEquals("DaAnswer!", myCache.get(42L));
    }

    public static void testEhcache3WithSerializationAndClientClass() {
      CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, Person.class, heap(10))
          .add(new DefaultCopierConfiguration<>(SerializingCopier.<Person>asCopierClass(), DefaultCopierConfiguration.Type.VALUE))
          .withClassLoader(TestMethods.class.getClassLoader())
          .build())
        .build(true);

      Cache<Long, Person> myCache = cacheManager.getCache("myCache", Long.class, Person.class);

      myCache.put(42L, new Person("Arthur"));
      assertTrue(myCache.get(42L) instanceof Person);
    }

    public static void testCustomCopier() {
      CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("myCache", newCacheConfigurationBuilder(Long.class, String.class, heap(10))
          .add(new DefaultCopierConfiguration<>(StringCopier.class, DefaultCopierConfiguration.Type.VALUE))
          .withClassLoader(TestMethods.class.getClassLoader())
          .build())
        .build(true);

      Cache<Long, String> cache = cacheManager.getCache("myCache", Long.class, String.class);

      cache.put(42L, "What's the question again?");
      cache.get(42L);
    }

    public static void testEhcacheXMLConfig() throws Exception {
      XmlConfiguration configuration = new XmlConfiguration(TestMethods.class.getResource("/org/ehcache/osgi/ehcache-osgi.xml").toURI().toURL(), TestMethods.class.getClassLoader());

      assertEquals(Person.class, configuration.getCacheConfigurations().get("bar").getValueType());
    }

    public static void testAllServicesAreAvailable() {
      Set<String> osgiAvailableClasses =
        stream(spliterator(OsgiServiceLoader.load(ServiceFactory.class).iterator(), Long.MAX_VALUE, 0), false)
          .map(f -> f.getClass().getName())
          .collect(toSet());

      Set<String> jdkAvailableClasses = of(EhcacheActivator.getCoreBundle().getBundles())
        .map(b -> b.adapt(BundleWiring.class).getClassLoader())
        .flatMap(cl ->
          stream(spliterator(ServiceLoader.load(ServiceFactory.class, cl).iterator(), Long.MAX_VALUE, 0), false)
            .map(f -> f.getClass().getName()))
        .collect(toSet());

      assertThat(osgiAvailableClasses, hasItems(jdkAvailableClasses.toArray(new String[0])));
    }

    public static class StringCopier extends ReadWriteCopier<String> {

      @Override
      public String copy(String obj) {
        return new String(obj);
      }
    }

  }
}
