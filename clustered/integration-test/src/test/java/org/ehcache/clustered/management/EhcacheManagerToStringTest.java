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

package org.ehcache.clustered.management;

import org.ehcache.CacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.HumanReadable;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertThat;

public class EhcacheManagerToStringTest extends AbstractClusteringManagementTest {

  @Test
  public void simpleOnHeapToString() throws Exception {
    File root = new File("build/tmp/EhcacheManagerToStringTest");
    root.mkdirs();

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(new CacheManagerPersistenceConfiguration(root))
      .withCache("cache-1", CacheConfigurationBuilder.newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .disk(2, MemoryUnit.MB, true))
        .withLoaderWriter(new SampleLoaderWriter<>())
        .withService(WriteBehindConfigurationBuilder
          .newBatchedWriteBehindConfiguration(1, TimeUnit.SECONDS, 3)
          .queueSize(3)
          .concurrencyLevel(1)
          .enableCoalescing())
        .withEvictionAdvisor((key, value) -> false)
        .build())
      .build(true)) {
      String actual = normalizeForLineEndings(((HumanReadable) cacheManager.getRuntimeConfiguration()).readableString());
      String expected = normalizeForLineEndings(read("/simpleConfiguration.txt"));

      // only testing part of the string, to avoid collections ordering clashes
      assertThat(
        actual.substring(actual.indexOf("resourcePools")).replace(" ", "").replace("\n", "").replaceAll("\\\\|/", "|"),
        equalTo(
          expected.substring(expected.indexOf("resourcePools"))
            .replace(" ", "")
            .replace("\n", "")
            .replaceAll("\\\\|/", "|")
        )
      );
    }
  }

  @Test
  public void clusteredToString() throws Exception {
    URI uri = CLUSTER.getCluster().getConnectionURI().resolve("/my-server-entity-2");

    try (CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      // cluster config
      .with(ClusteringServiceConfigurationBuilder.cluster(uri)
        .autoCreate(server -> server.defaultServerResource("primary-server-resource")
        .resourcePool("resource-pool-a", 10, MemoryUnit.MB)))
      // management config
      .using(new DefaultManagementRegistryConfiguration()
        .addTags("webapp-1", "server-node-1")
        .setCacheManagerAlias("my-super-cache-manager"))
      // cache clustered dedicated
      .withCache("cache-dedicated", CacheConfigurationBuilder.newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(10, EntryUnit.ENTRIES)
          .offheap(1, MemoryUnit.MB)
          .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
        .build())
      // cache clustered shared
      .withCache("cache-shared", CacheConfigurationBuilder.newCacheConfigurationBuilder(
        String.class, String.class,
        newResourcePoolsBuilder()
          .heap(1, MemoryUnit.MB)
          .with(ClusteredResourcePoolBuilder.clusteredShared("resource-pool-a")))
        .build())
      .build(true)) {
      String actual = normalizeForLineEndings(((HumanReadable) cacheManager.getRuntimeConfiguration()).readableString());
      String expected = normalizeForLineEndings(read("/clusteredConfiguration.txt"));

      // only testing part of the string, to avoid collections ordering clashes
      assertThat(
        actual.substring(actual.indexOf("resourcePools")).replace(" ", "").replace("\n", ""),
        equalTo(
          expected.substring(expected.indexOf("resourcePools"))
            .replace(" ", "")
            .replace("\n", "")
            .replace("server-1", uri.getAuthority())
        )
      );

      assertThat(actual.indexOf("serviceConfigurations: None"), greaterThan(1));
      assertThat(actual.indexOf("evictionAdvisor: None"), greaterThan(1));
    }
  }


  public static class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

    @Override
    public V load(K key) {
      throw new UnsupportedOperationException("Implement Me");
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void write(K key, V value) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void delete(K key) {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void deleteAll(Iterable<? extends K> keys) {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

}
