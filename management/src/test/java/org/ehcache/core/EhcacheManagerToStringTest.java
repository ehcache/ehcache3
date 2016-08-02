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

package org.ehcache.core;

import org.ehcache.CacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.EhcacheClientEntityService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.EhcacheServerEntityService;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.terracotta.management.entity.management.client.ManagementAgentEntityClientService;
import org.terracotta.management.entity.management.server.ManagementAgentEntityServerService;
import org.terracotta.management.service.monitoring.IMonitoringConsumer;
import org.terracotta.offheapresource.OffHeapResourcesConfiguration;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.OffheapResourcesType;
import org.terracotta.offheapresource.config.ResourceType;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.math.BigInteger;
import java.net.URI;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class EhcacheManagerToStringTest {

  static IMonitoringConsumer consumer;
  static PassthroughClusterControl stripeControl;

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

    String actual = ((HumanReadable) cacheManager.getRuntimeConfiguration()).readableString();
    String expected = read("/simpleConfiguration.txt");

    // only testing part of the string, to avoid collections ordering clashes
    Assert.assertThat(
        actual.substring(actual.indexOf("resourcePools")).replace(" ", "").replace("\n", "").replaceAll("\\\\|/", "|"),
        equalTo(
            expected.substring(expected.indexOf("resourcePools")).replace(" ", "").replace("\n", "").replaceAll("\\\\|/", "|")
        )
    );
  }

  @Test
  public void clusteredToString() throws Exception {
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        // cluster config
        .with(ClusteringServiceConfigurationBuilder.cluster(URI.create("passthrough://server-1:9510/my-server-entity-1"))
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
                .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 1, MemoryUnit.MB)))
            .build())
        .build(true);

    String actual = ((HumanReadable) cacheManager.getRuntimeConfiguration()).readableString();
    String expected = read("/clusteredConfiguration.txt");

    System.out.println(actual);

    // only testing part of the string, to avoid collections ordering clashes
    Assert.assertThat(
        actual.substring(actual.indexOf("resourcePools")).replace(" ", "").replace("\n", ""),
        equalTo(
            expected.substring(expected.indexOf("resourcePools")).replace(" ", "").replace("\n", "")
        )
    );

    Assert.assertThat(actual.indexOf("serviceConfigurations: None"), greaterThan(1));
    Assert.assertThat(actual.indexOf("evictionAdvisor: None"), greaterThan(1));

    if (cacheManager != null && cacheManager.getStatus() == Status.AVAILABLE) {
      cacheManager.close();
    }
  }


  @BeforeClass
  public static void beforeClass() throws Exception {
    PassthroughServer activeServer = new PassthroughServer();
    activeServer.setServerName("server-1");
    activeServer.setBindPort(9510);
    activeServer.setGroupPort(9610);

    // management agent entity
    activeServer.registerServerEntityService(new ManagementAgentEntityServerService());
    activeServer.registerClientEntityService(new ManagementAgentEntityClientService());

    // ehcache entity
    activeServer.registerServerEntityService(new EhcacheServerEntityService());
    activeServer.registerClientEntityService(new EhcacheClientEntityService());

    // RW lock entity (required by ehcache)
    activeServer.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
    activeServer.registerClientEntityService(new VoltronReadWriteLockEntityClientService());

    // off-heap service
    OffheapResourcesType offheapResourcesType = new OffheapResourcesType();
    ResourceType resourceType = new ResourceType();
    resourceType.setName("primary-server-resource");
    resourceType.setUnit(org.terracotta.offheapresource.config.MemoryUnit.MB);
    resourceType.setValue(BigInteger.TEN);
    offheapResourcesType.getResource().add(resourceType);
    activeServer.registerServiceProvider(new OffHeapResourcesProvider(), new OffHeapResourcesConfiguration(offheapResourcesType));

    stripeControl = new PassthroughClusterControl("server-1", activeServer);
  }

  @AfterClass
  public static void afterClass() throws Exception {
    stripeControl.tearDown();
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

  private String read(String path) throws FileNotFoundException {
    Scanner scanner = new Scanner(getClass().getResourceAsStream(path), "UTF-8");
    try {
      return scanner.useDelimiter("\\A").next();
    } finally {
      scanner.close();
    }
  }

}
