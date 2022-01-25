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
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.HumanReadable;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.URI;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;

public class EhcacheManagerToStringTest extends AbstractClusteringManagementTest {

  @Test
  public void simpleOnHeapToString() throws Exception {
    File root = new File("build/tmp/EhcacheManagerToStringTest");
    root.mkdirs();

    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .with(new CacheManagerPersistenceConfiguration(root))
        .withCache("cache-1", CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .disk(2, MemoryUnit.MB, true))
            .withLoaderWriter(new SampleLoaderWriter<String, String>())
            .add(WriteBehindConfigurationBuilder
                .newBatchedWriteBehindConfiguration(1, TimeUnit.SECONDS, 3)
                .queueSize(3)
                .concurrencyLevel(1)
                .enableCoalescing())
            .withEvictionAdvisor(new EvictionAdvisor<String, String>() {
              @Override
              public boolean adviseAgainstEviction(String key, String value) {
                return false;
              }
            })
            .build())
        .build(true);

    try {
      String actual = normalizeForLineEndings(((HumanReadable) cacheManager.getRuntimeConfiguration()).readableString());
      String expected = normalizeForLineEndings(read("/simpleConfiguration.txt"));

      // only testing part of the string, to avoid collections ordering clashes
      assertThat(
          actual.substring(actual.indexOf("resourcePools")).replace(" ", "").replace("\n", "").replaceAll("\\\\|/", "|"),
          equalTo(
              expected.substring(expected.indexOf("resourcePools")).replace(" ", "").replace("\n", "").replaceAll("\\\\|/", "|")
          )
      );
    } finally {
      cacheManager.close();
    }
  }

  @Test
  public void clusteredToString() throws Exception {
    URI uri = CLUSTER.getConnectionURI().resolve("/my-server-entity-2");
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        // cluster config
        .with(ClusteringServiceConfigurationBuilder.cluster(uri)
            .autoCreate()
            .defaultServerResource("primary-server-resource"))
        // management config
        .using(new DefaultManagementRegistryConfiguration()
            .addTags("webapp-1", "server-node-1")
            .setCacheManagerAlias("my-super-cache-manager")
            .addConfiguration(new EhcacheStatisticsProviderConfiguration(
                1, TimeUnit.MINUTES,
                100, 1, TimeUnit.SECONDS,
                2, TimeUnit.SECONDS))) // TTD reduce to 2 seconds so that the stat collector run faster
        // cache config
        .withCache("cache-1", CacheConfigurationBuilder.newCacheConfigurationBuilder(
            String.class, String.class,
            newResourcePoolsBuilder()
                .heap(10, EntryUnit.ENTRIES)
                .offheap(1, MemoryUnit.MB)
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
            .build())
        .build(true);

    try {
      String actual = normalizeForLineEndings(((HumanReadable) cacheManager.getRuntimeConfiguration()).readableString());
      String expected = normalizeForLineEndings(read("/clusteredConfiguration.txt"));

      // only testing part of the string, to avoid collections ordering clashes
      assertThat(
          actual.substring(actual.indexOf("resourcePools")).replace(" ", "").replace("\n", ""),
          equalTo(
              expected.substring(expected.indexOf("resourcePools")).replace(" ", "").replace("\n", "").replace("server-1:9510", uri.getAuthority())
          )
      );

      assertThat(actual.indexOf("serviceConfigurations: None"), greaterThan(1));
      assertThat(actual.indexOf("evictionAdvisor: None"), greaterThan(1));
    } finally {
      cacheManager.close();
    }
  }


  public static class SampleLoaderWriter<K, V> implements CacheLoaderWriter<K, V> {

    @Override
    public V load(K key) throws Exception {
      throw new UnsupportedOperationException("Implement Me");
    }

    @Override
    public Map<K, V> loadAll(Iterable<? extends K> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void write(K key, V value) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void writeAll(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void delete(K key) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }

    @Override
    public void deleteAll(Iterable<? extends K> keys) throws Exception {
      throw new UnsupportedOperationException("Implement me!");
    }
  }

}
