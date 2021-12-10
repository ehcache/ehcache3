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
package org.ehcache.clustered.client.internal.store.lock;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.clustered.client.internal.ClusterTierManagerClientEntityService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.lock.VoltronReadWriteLockEntityClientService;
import org.ehcache.clustered.client.internal.store.ClusterTierClientEntityService;
import org.ehcache.clustered.lock.server.VoltronReadWriteLockServerEntityService;
import org.ehcache.clustered.server.ObservableEhcacheServerEntityService;
import org.ehcache.clustered.server.store.ObservableClusterTierServerEntityService;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.offheapresource.OffHeapResourcesProvider;
import org.terracotta.offheapresource.config.MemoryUnit;
import org.terracotta.passthrough.PassthroughClusterControl;
import org.terracotta.passthrough.PassthroughTestHelpers;

import java.net.URI;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder.cluster;
import static org.ehcache.clustered.client.internal.UnitTestConnectionService.getOffheapResourcesType;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class LockRetentionDuringFailoverTest {

  private static final String STRIPENAME = "stripe";
  private static final String STRIPE_URI = "passthrough://" + STRIPENAME;

  private PassthroughClusterControl clusterControl;

  private CountDownLatch latch;
  private LatchedLoaderWriter loaderWriter;
  private CacheManager cacheManager;
  private Cache<Long, String> cache;

  @Before
  public void setUp() throws Exception {
    this.clusterControl = PassthroughTestHelpers.createActivePassive(STRIPENAME,
            server -> {
              server.registerServerEntityService(new ObservableEhcacheServerEntityService());
              server.registerClientEntityService(new ClusterTierManagerClientEntityService());
              server.registerServerEntityService(new ObservableClusterTierServerEntityService());
              server.registerClientEntityService(new ClusterTierClientEntityService());
              server.registerServerEntityService(new VoltronReadWriteLockServerEntityService());
              server.registerClientEntityService(new VoltronReadWriteLockEntityClientService());
              server.registerExtendedConfiguration(new OffHeapResourcesProvider(getOffheapResourcesType("test", 32, MemoryUnit.MB)));

              UnitTestConnectionService.addServerToStripe(STRIPENAME, server);
            }
    );

    clusterControl.waitForActive();
    clusterControl.waitForRunningPassivesInStandby();

    this.latch = new CountDownLatch(1);
    this.loaderWriter = new LatchedLoaderWriter(latch);

    CacheConfiguration<Long, String> config = CacheConfigurationBuilder
            .newCacheConfigurationBuilder(Long.class, String.class,
            newResourcePoolsBuilder()
                    .with(clusteredDedicated("test", 2, org.ehcache.config.units.MemoryUnit.MB)))
            .withLoaderWriter(loaderWriter)
            .build();

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder().with(cluster(URI.create(STRIPE_URI)).autoCreate(c -> c))
            .withCache("cache-1", config)
            .build(true);

    cache = cacheManager.getCache("cache-1", Long.class, String.class);

  }

  @After
  public void tearDown() throws Exception {
    try {
      cacheManager.close();
    } finally {
      UnitTestConnectionService.removeStripe(STRIPENAME);
      clusterControl.tearDown();
    }
  }

  @Test
  public void testLockRetentionDuringFailover() throws Exception {

    ExecutorService executorService = Executors.newFixedThreadPool(1);
    Future<?> putFuture = executorService.submit(() -> cache.put(1L, "one"));

    clusterControl.terminateActive();
    clusterControl.waitForActive();

    assertThat(loaderWriter.backingMap.isEmpty(), is(true));

    latch.countDown();

    putFuture.get();

    assertThat(loaderWriter.backingMap.get(1L), is("one"));

  }

  private static class LatchedLoaderWriter implements CacheLoaderWriter<Long, String> {

    ConcurrentHashMap<Long, String> backingMap = new ConcurrentHashMap<>();
    private final CountDownLatch latch;

    LatchedLoaderWriter(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public String load(Long key) throws Exception {
      latch.await();
      return backingMap.get(key);
    }

    @Override
    public void write(Long key, String value) throws Exception {
      latch.await();
      backingMap.put(key, value);
    }

    @Override
    public void delete(Long key) throws Exception {
      latch.await();
      backingMap.remove(key);
    }
  }

}
