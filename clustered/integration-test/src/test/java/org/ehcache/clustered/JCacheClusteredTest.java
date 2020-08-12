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

import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.net.URL;

import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import static org.ehcache.clustered.CacheManagerLifecycleEhcacheIntegrationTest.substitute;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

/**
 * JCacheClusteredTest
 */
public class JCacheClusteredTest extends ClusteredTests {

  private static final String RESOURCE_CONFIG =
      "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER = newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
  }

  @Test
  public void testJCacheClustered() throws Exception {
    URL xml = CacheManagerLifecycleEhcacheIntegrationTest.class.getResource("/configs/jcache-clustered.xml");
    URL substitutedXml = substitute(xml, "cluster-uri", CLUSTER.getConnectionURI().toString());
    CachingProvider cachingProvider = Caching.getCachingProvider();
    cachingProvider.getCacheManager(substitutedXml.toURI(), getClass().getClassLoader());
  }
}
