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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.units.MemoryUnit;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static java.util.stream.LongStream.range;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.CacheManagerBuilder.newCacheManagerBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

public class ClusteredIterationTest extends ClusteredTests {

  private static final String RESOURCE_CONFIG =
    "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
      + "<ohr:offheap-resources>"
      + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
      + "</ohr:offheap-resources>" +
      "</config>\n";

  @ClassRule
  public static Cluster CLUSTER =
    newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @Rule
  public final TestName testName = new TestName();

  @Test
  public void testIterationTerminatedWithException() {
    try (CacheManager cacheManager = createTestCacheManager()) {
      Cache<Long, byte[]> cache = cacheManager.getCache(testName.getMethodName(), Long.class, byte[].class);

      byte[] data = new byte[101 * 1024];
      cache.put(1L, data);
      cache.put(2L, data);

      Iterator<Cache.Entry<Long, byte[]>> iterator = cache.iterator();

      assertThat(iterator.next(), notNullValue());
      assertThat(iterator.next(), notNullValue());

      try {
        iterator.next();
        fail("Expected NoSuchElementException");
      } catch (NoSuchElementException e) {
        //expected
      }
    }
  }

  @Test
  public void testIterationWithSingleLastBatchIsBroken() {
    try (CacheManager cacheManager = createTestCacheManager()) {
      Cache<Long, byte[]> cache = cacheManager.getCache(testName.getMethodName(), Long.class, byte[].class);

      byte[] data = new byte[101 * 1024];
      cache.put(1L, data);
      cache.put(2L, data);

      for (Cache.Entry<Long, byte[]> entry : cache) {
        assertThat(entry, notNullValue());
      }
    }
  }

  @Test
  public void testIterationWithConcurrentClearedCacheException() {
    try (CacheManager cacheManager = createTestCacheManager()) {
      Cache<Long, byte[]> cache = cacheManager.getCache(testName.getMethodName(), Long.class, byte[].class);

      byte[] data = new byte[10 * 1024];
      range(0, 20).forEach(k -> cache.put(k, data));

      Iterator<Cache.Entry<Long, byte[]>> iterator = cache.iterator();

      cache.clear();
      try {
        while (true) {
          iterator.next();
        }
      } catch (NoSuchElementException e) {
        //expected
      }
    }
  }

  private CacheManager createTestCacheManager() {
    return newCacheManagerBuilder().with(cluster(CLUSTER.getConnectionURI().resolve("/iteration-cm"))
      .autoCreate().defaultServerResource("primary-server-resource"))
      .withCache(testName.getMethodName(), newCacheConfigurationBuilder(Long.class, byte[].class, newResourcePoolsBuilder()
          .with(clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))).build(true);
  }
}
