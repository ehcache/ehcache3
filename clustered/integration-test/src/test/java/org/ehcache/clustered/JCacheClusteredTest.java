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

package org.ehcache.clustered;

import org.ehcache.testing.ExternalTests;
import org.jsr107.tck.spi.CachingProviderTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.net.URL;
import java.util.Properties;

import static org.ehcache.clustered.CacheManagerLifecycleEhcacheIntegrationTest.substitute;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

/**
 * JCacheClusteredTest - runs the TCK test suite using clustered caches
 */
@RunWith(ExternalTests.class)
@ExternalTests.From(javax.cache.CachingTest.class)
@ExternalTests.Ignore(value=CachingProviderTest.class, method="getCacheManagerUsingDefaultURI")
public class JCacheClusteredTest extends ClusteredTests {

  private static final Properties TCK_PROPERTIES = new Properties();
  static {
    TCK_PROPERTIES.setProperty("java.net.preferIPv4Stack", "true");
    TCK_PROPERTIES.setProperty("javax.management.builder.initial", "org.ehcache.jsr107.internal.tck.Eh107MBeanServerBuilder");
    TCK_PROPERTIES.setProperty("org.jsr107.tck.management.agentId", "Eh107MBeanServer");
    TCK_PROPERTIES.setProperty("javax.cache.CacheManager", "org.ehcache.CacheManager");
    TCK_PROPERTIES.setProperty("javax.cache.Cache", "org.ehcache.Cache");
    TCK_PROPERTIES.setProperty("javax.cache.Cache.Entry", "org.ehcache.Cache$Entry");
    TCK_PROPERTIES.setProperty("javax.cache.annotation.CacheInvocationContext", "javax.cache.annotation.impl.cdi.CdiCacheKeyInvocationContextImpl");
  }
  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary\" unit=\"MB\">256</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    URL xml = CacheManagerLifecycleEhcacheIntegrationTest.class.getResource("/configs/jcache-clustered.xml");
    URL substitutedXml = substitute(xml, "cluster-uri", CLUSTER.getConnectionURI().toString());
    System.setProperty("ehcache.jsr107.config.default", substitutedXml.toURI().toString());
    TCK_PROPERTIES.forEach((k, v) -> System.setProperty(k.toString(), v.toString()));
    CLUSTER.getClusterControl().waitForActive();
  }

  @AfterClass
  public static void cleanup() {
    System.clearProperty("ehcache.jsr107.config.default");
    TCK_PROPERTIES.forEach((k, v) -> System.clearProperty(k.toString()));
  }
}
