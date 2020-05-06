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
import org.jsr107.tck.event.CacheListenerTest;
import org.jsr107.tck.spi.CachingProviderTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.runner.RunWith;
import org.terracotta.testing.rules.Cluster;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;
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
@ExternalTests.Ignore(value= CacheListenerTest.class)
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
  @ClassRule
  public static Cluster CLUSTER = newCluster().in(clusterPath())
    .withServiceFragment(offheapResource("primary", 256)).build();

  @BeforeClass
  public static void configureEnvironment() throws Exception {
    URL xml = CacheManagerLifecycleEhcacheIntegrationTest.class.getResource("/configs/jcache-clustered.xml");
    URL substitutedXml = substitute(xml, "cluster-uri", CLUSTER.getConnectionURI().toString());
    System.setProperty("ehcache.jsr107.config.default", substitutedXml.toURI().toString());
    TCK_PROPERTIES.forEach((k, v) -> System.setProperty(k.toString(), v.toString()));
  }

  @AfterClass
  public static void cleanup() {
    try {
      Caching.getCachingProviders().forEach(CachingProvider::close);
    } finally {
      System.clearProperty("ehcache.jsr107.config.default");
      TCK_PROPERTIES.forEach((k, v) -> System.clearProperty(k.toString()));
    }
  }
}
