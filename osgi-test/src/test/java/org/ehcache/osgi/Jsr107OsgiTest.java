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

import org.ehcache.core.osgi.EhcacheActivator;
import org.ehcache.core.osgi.OsgiServiceLoader;
import org.ehcache.core.spi.service.ServiceFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.wiring.BundleWiring;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import java.util.ServiceLoader;
import java.util.Set;

import static java.util.Spliterators.spliterator;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static java.util.stream.StreamSupport.stream;
import static org.ehcache.osgi.OsgiTestUtils.baseConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.gradleBundle;
import static org.ehcache.osgi.OsgiTestUtils.jaxbConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.wrappedGradleBundle;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.ops4j.pax.exam.CoreOptions.options;

/**
 * Jsr107OsgiTest
 */
@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class Jsr107OsgiTest {

  @Configuration
  public Option[] individualModules() {
    return options(
      gradleBundle("org.ehcache.modules:impl"),
      gradleBundle("org.ehcache.modules:xml"), jaxbConfiguration(),
      gradleBundle("org.ehcache.modules:107"),
      gradleBundle("org.ehcache.modules:core"),
      gradleBundle("org.ehcache.modules:api"),
      gradleBundle("javax.cache:cache-api"),

      gradleBundle("org.terracotta.management:management-model"),
      gradleBundle("org.terracotta.management:sequence-generator"),

      wrappedGradleBundle("org.terracotta:statistics"),
      wrappedGradleBundle("org.ehcache:sizeof"),
      wrappedGradleBundle("org.terracotta:offheap-store"),
      wrappedGradleBundle("org.terracotta:terracotta-utilities-tools"),

      baseConfiguration("Jsr107OsgiTest", "individualModules")
    );
  }

  @Configuration
  public Option[] uberJar() {
    return options(
      gradleBundle("org.ehcache:dist"), jaxbConfiguration(),
      gradleBundle("javax.cache:cache-api"),

      baseConfiguration("Jsr107OsgiTest", "uberJar")
    );
  }

  @Test
  public void testJsr107EhcacheOsgi() throws Exception {
    TestMethods.testJsr107EhcacheOsgi();
  }

  @Test
  public void testAllServicesAreAvailable() {
    TestMethods.testAllServicesAreAvailable();
  }

  private static class TestMethods {
    @SuppressWarnings("unchecked")
    public static void testJsr107EhcacheOsgi() throws Exception {
      CachingProvider cachingProvider = Caching.getCachingProvider("org.ehcache.jsr107.EhcacheCachingProvider", TestMethods.class.getClassLoader());
      CacheManager cacheManager = cachingProvider.getCacheManager(TestMethods.class.getResource("/org/ehcache/osgi/ehcache-107-osgi.xml").toURI(), TestMethods.class.getClassLoader());
      Cache<Long, Person> personCache = cacheManager.getCache("personCache", Long.class, Person.class);
      assertEquals(Person.class, personCache.getConfiguration(javax.cache.configuration.Configuration.class).getValueType());
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
  }
}
