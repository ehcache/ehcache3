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
import org.ehcache.PersistentCacheManager;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.osgi.EhcacheActivator;
import org.ehcache.core.osgi.OsgiServiceLoader;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.ops4j.pax.exam.Configuration;
import org.ops4j.pax.exam.Option;
import org.ops4j.pax.exam.junit.PaxExam;
import org.ops4j.pax.exam.spi.reactors.ExamReactorStrategy;
import org.ops4j.pax.exam.spi.reactors.PerMethod;
import org.osgi.framework.wiring.BundleWiring;
import org.terracotta.org.junit.rules.TemporaryFolder;
import org.w3c.dom.Document;
import org.w3c.dom.Node;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathFactory;
import java.io.File;
import java.util.ServiceLoader;
import java.util.Set;

import static java.util.Spliterators.spliterator;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.Stream.of;
import static java.util.stream.StreamSupport.stream;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.osgi.OsgiTestUtils.baseConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.gradleBundle;
import static org.ehcache.osgi.OsgiTestUtils.jaxbConfiguration;
import static org.ehcache.osgi.OsgiTestUtils.startServer;
import static org.ehcache.osgi.OsgiTestUtils.wrappedGradleBundle;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertThat;
import static org.ops4j.pax.exam.CoreOptions.options;

@RunWith(PaxExam.class)
@ExamReactorStrategy(PerMethod.class)
public class ClusteredOsgiTest {

  @Rule
  public TemporaryFolder serverLocation = new TemporaryFolder();

  @Configuration
  public Option[] individualModules() {
    return options(
      gradleBundle("org.ehcache.modules:api"),
      gradleBundle("org.ehcache.modules:core"),
      gradleBundle("org.ehcache.modules:impl"),
      gradleBundle("org.ehcache.modules:xml"), jaxbConfiguration(),
      gradleBundle("org.ehcache:clustered-dist"),

      gradleBundle("org.terracotta.management:management-model"),
      gradleBundle("org.terracotta.management:sequence-generator"),

      wrappedGradleBundle("org.terracotta:statistics"),
      wrappedGradleBundle("org.ehcache:sizeof"),
      wrappedGradleBundle("org.terracotta:offheap-store"),
      wrappedGradleBundle("org.terracotta:terracotta-utilities-tools"),

      baseConfiguration("ClusteredOsgiTest", "individualModules")
    );
  }

  @Configuration
  public Option[] uberJar() {
    return options(
      gradleBundle("org.ehcache:dist"), jaxbConfiguration(),
      gradleBundle("org.ehcache:clustered-dist"),

      baseConfiguration("ClusteredOsgiTest", "uberJar")
    );
  }

  @Test
  public void testProgrammaticClusteredCache() throws Throwable {
    try (OsgiTestUtils.Cluster cluster = startServer(serverLocation.newFolder().toPath())) {
      TestMethods.testProgrammaticClusteredCache(cluster);
    }
  }

  @Test
  public void testXmlClusteredCache() throws Throwable {
    try (OsgiTestUtils.Cluster cluster = startServer(serverLocation.newFolder().toPath())) {
      TestMethods.testXmlClusteredCache(cluster);
    }
  }

  @Test
  public void testAllServicesAreAvailable() {
    TestMethods.testAllServicesAreAvailable();
  }

  private static class TestMethods {

    public static void testProgrammaticClusteredCache(OsgiTestUtils.Cluster cluster) throws Throwable {
      try (PersistentCacheManager cacheManager = newCacheManagerBuilder()
        .with(cluster(cluster.getConnectionUri()).autoCreate(c -> c))
        .withCache("clustered-cache", newCacheConfigurationBuilder(Long.class, String.class,
          newResourcePoolsBuilder().with(clusteredDedicated("main", 2, MemoryUnit.MB))))
        .build(true)) {

        final Cache<Long, String> cache = cacheManager.getCache("clustered-cache", Long.class, String.class);

        cache.put(1L, "value");
        assertThat(cache.get(1L), is("value"));
      }
    }

    public static void testXmlClusteredCache(OsgiTestUtils.Cluster cluster) throws Exception {
      File config = cluster.getWorkingArea().resolve("ehcache.xml").toFile();

      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(TestMethods.class.getResourceAsStream("ehcache-clustered-osgi.xml"));
      XPath xpath = XPathFactory.newInstance().newXPath();
      Node clusterUriAttribute = (Node) xpath.evaluate("//config/service/cluster/connection/@url", doc, XPathConstants.NODE);
      clusterUriAttribute.setTextContent(cluster.getConnectionUri().toString() + "/cache-manager");
      Transformer xformer = TransformerFactory.newInstance().newTransformer();
      xformer.transform(new DOMSource(doc), new StreamResult(config));


      try (PersistentCacheManager cacheManager = (PersistentCacheManager) CacheManagerBuilder.newCacheManager(
        new XmlConfiguration(config.toURI().toURL(), TestMethods.class.getClassLoader())
      )) {
        cacheManager.init();

        final Cache<Long, Person> cache = cacheManager.getCache("clustered-cache", Long.class, Person.class);

        cache.put(1L, new Person("Brian"));
        assertThat(cache.get(1L).name, is("Brian"));
      }
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
