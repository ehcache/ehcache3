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
import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.util.TestCacheLoaderWriter;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.resilience.ThrowingResilienceStrategy;
import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryService;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.terracotta.testing.rules.Cluster;

import java.io.File;
import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.terracotta.testing.rules.BasicExternalClusterBuilder.newCluster;

@RunWith(Parameterized.class)
public class ClusteredLoaderWriterTest extends ClusteredTests {

  private static final String RESOURCE_CONFIG =
          "<config xmlns:ohr='http://www.terracotta.org/config/offheap-resource'>"
            + "<ohr:offheap-resources>"
            + "<ohr:resource name=\"primary-server-resource\" unit=\"MB\">64</ohr:resource>"
            + "</ohr:offheap-resources>" +
            "</config>\n";

  @Parameterized.Parameters(name = "consistency={0}")
  public static Consistency[] data() {
    return Consistency.values();
  }

  @Parameterized.Parameter
  public Consistency cacheConsistency;

  private static CacheManager cacheManager;
  private Cache<Long, String> client1;
  private CacheConfiguration<Long, String> configuration;

  private ConcurrentMap<Long, String> sor;

  @ClassRule
  public static Cluster CLUSTER =
          newCluster().in(new File("build/cluster")).withServiceFragment(RESOURCE_CONFIG).build();

  @BeforeClass
  public static void waitForActive() throws Exception {
    CLUSTER.getClusterControl().waitForActive();
    cacheManager = newCacheManager();
  }

  private static PersistentCacheManager newCacheManager() {
    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager1");
    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(registryConfiguration);
    return CacheManagerBuilder.newCacheManagerBuilder()
            .with(cluster(CLUSTER.getConnectionURI())
                    .timeouts(TimeoutsBuilder.timeouts()
                                             .read(Duration.ofSeconds(30))
                                             .write(Duration.ofSeconds(30)))
                    .autoCreate()
                    .build())
            .using(managementRegistry)
            .build(true);
  }

  @Before
  public void setUp() throws Exception {

    sor = new ConcurrentHashMap<>();
    configuration = getCacheConfig();
  }

  private CacheConfiguration<Long, String> getCacheConfig() {
    return CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
                    ResourcePoolsBuilder
                            .heap(20)
                            .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
            .withLoaderWriter(new TestCacheLoaderWriter(sor))
            .add(ClusteredStoreConfigurationBuilder.withConsistency(cacheConsistency))
            .withResilienceStrategy(new ThrowingResilienceStrategy<>())
            .build();
  }

  @Test
  public void testBasicOps() {
    client1 = cacheManager.createCache("basicops" + cacheConsistency.name(), configuration);
    assertThat(sor.isEmpty(), is(true));

    Set<Long> keys = new HashSet<>();
    ThreadLocalRandom.current().longs(10).forEach(x -> {
      keys.add(x);
      client1.put(x, Long.toString(x));
    });

    assertThat(sor.size(), is(10));

    CacheManager anotherCacheManager = newCacheManager();
    Cache<Long, String> client2 = anotherCacheManager.createCache("basicops" + cacheConsistency.name(),
            getCacheConfig());
    Map<Long, String> all = client2.getAll(keys);
    assertThat(all.keySet(), containsInAnyOrder(keys.toArray()));

    keys.stream().limit(3).forEach(client2::remove);

    assertThat(sor.size(), is(7));
  }

  @Test
  public void testCASOps() {
    client1 = cacheManager.createCache("casops" + cacheConsistency.name(), configuration);
    assertThat(sor.isEmpty(), is(true));

    Set<Long> keys = new HashSet<>();
    ThreadLocalRandom.current().longs(10).forEach(x -> {
      keys.add(x);
      client1.put(x, Long.toString(x));
    });
    assertThat(sor.size(), is(10));

    CacheManager anotherCacheManager = newCacheManager();
    Cache<Long, String> client2 = anotherCacheManager.createCache("casops" + cacheConsistency.name(),
            getCacheConfig());

    keys.forEach(x -> assertThat(client2.putIfAbsent(x, "Again" + x), is(Long.toString(x))));

    assertThat(sor.size(), is(10));

    keys.stream().limit(5).forEach(x ->
            assertThat(client2.replace(x , "Replaced" + x), is(Long.toString(x))));

    assertThat(sor.size(), is(10));

    keys.forEach(x -> client1.remove(x, Long.toString(x)));

    assertThat(sor.size(), is(5));

    AtomicInteger success = new AtomicInteger(0);

    keys.forEach(x -> {
      if (client2.replace(x, "Replaced" + x, "Again")) {
        success.incrementAndGet();
      }
    });

    assertThat(success.get(), is(5));

  }

}
