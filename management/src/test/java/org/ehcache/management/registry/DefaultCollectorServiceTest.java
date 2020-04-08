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
package org.ehcache.management.registry;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.management.CollectorService;
import org.ehcache.management.ManagementRegistryService;
import org.junit.Test;
import org.terracotta.management.model.call.Parameter;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.notification.ContextualNotification;
import org.terracotta.management.model.stats.ContextualStatistics;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DefaultCollectorServiceTest {

  @Test
  public void test_collector() throws Exception {
    final Queue<Object> messages = new ConcurrentLinkedQueue<>();
    final List<String> notifs = new ArrayList<>(7);
    final CountDownLatch num = new CountDownLatch(6);

    CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB))
        .build();

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration()
        .setCacheManagerAlias("my-cm-1"));

    CollectorService collectorService = new DefaultCollectorService(new CollectorService.Collector() {
      @Override
      public void onNotification(ContextualNotification notification) {
        onEvent(notification);
      }

      @Override
      public void onStatistics(Collection<ContextualStatistics> statistics) {
        onEvent(statistics);
      }

      void onEvent(Object event) {
        messages.offer(event);
        if (event instanceof ContextualNotification) {
          notifs.add(((ContextualNotification) event).getType());
        }
        num.countDown();
      }
    });

    // ehcache cache manager
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(managementRegistry)
        .using(collectorService)
        .build(false);

    cacheManager.init();

    cacheManager.close();
    cacheManager.init();

    managementRegistry.withCapability("StatisticCollectorCapability")
        .call("startStatisticCollector",
          new Parameter(1L, long.class.getName()),
          new Parameter(TimeUnit.SECONDS, TimeUnit.class.getName()))
        .on(Context.create("cacheManagerName", "my-cm-1"))
        .build()
        .execute()
        .getSingleResult();

    Cache<String, String> cache = cacheManager.createCache("my-cache", cacheConfiguration);
    cache.put("key", "val");
    cache.clear();

    num.await(10, TimeUnit.SECONDS);
    cacheManager.removeCache("my-cache");
    cacheManager.close();

    assertThat(notifs, equalTo(Arrays.asList("CACHE_MANAGER_AVAILABLE", "CACHE_MANAGER_CLOSED", "CACHE_MANAGER_AVAILABLE", "CACHE_ADDED", "CACHE_CLEARED", "CACHE_REMOVED", "CACHE_MANAGER_CLOSED")));
    assertThat(messages.size(), equalTo(8));
  }

}
