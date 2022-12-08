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

package org.ehcache.clustered.client;

import org.ehcache.PersistentCacheManager;
import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This test makes sure a clustered cache can be opened from many client instances. As usual with concurrency tests, a
 * success doesn't mean it will work forever and a failure might not occur reliably. However, it puts together all
 * conditions to make it fail in case of race condition
 *
 * @author Henri Tremblay
 */
public class ClusteredConcurrencyTest {

  private static final URI CLUSTER_URI = URI.create("terracotta://example.com:9540/my-application");
  private static final String CACHE_NAME = "clustered-cache";

  private AtomicReference<Throwable> exception = new AtomicReference<>();

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI,
      new UnitTestConnectionService.PassthroughServerBuilder()
        .resource("primary-server-resource", 64, MemoryUnit.MB)
        .resource("secondary-server-resource", 64, MemoryUnit.MB)
        .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI);
  }

  @Test
  public void test() throws Throwable {
    final int THREAD_NUM = 50;

    final CountDownLatch latch = new CountDownLatch(THREAD_NUM + 1);

    List<Thread> threads = new ArrayList<>(THREAD_NUM);
    for (int i = 0; i < THREAD_NUM; i++) {
      Thread t1 = new Thread(content(latch));
      t1.start();
      threads.add(t1);
    }

    latch.countDown();
    latch.await();

    for(Thread t : threads) {
      t.join();
    }

    Throwable throwable = exception.get();
    if(throwable != null) {
      throw throwable;
    }
  }

  private Runnable content(final CountDownLatch latch) {
    return () -> {
      try {
        CacheManagerBuilder<PersistentCacheManager> clusteredCacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder()
          .with(ClusteringServiceConfigurationBuilder.cluster(CLUSTER_URI)
            .autoCreate(server -> server.defaultServerResource("primary-server-resource")
              .resourcePool("resource-pool-a", 8, MemoryUnit.MB)
              .resourcePool("resource-pool-b", 8, MemoryUnit.MB, "secondary-server-resource")))
          .withCache(CACHE_NAME, CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
            ResourcePoolsBuilder.newResourcePoolsBuilder()
              .with(ClusteredResourcePoolBuilder.clusteredDedicated("primary-server-resource", 8, MemoryUnit.MB)))
            .withService(new ClusteredStoreConfiguration(Consistency.STRONG)));

        latch.countDown();
        try {
          latch.await();
        } catch (InterruptedException e) {
          // continue
        }

        clusteredCacheManagerBuilder.build(true).close();
      } catch (Throwable t) {
        exception.compareAndSet(null, t); // only keep the first exception
      }
    };
  }
}
