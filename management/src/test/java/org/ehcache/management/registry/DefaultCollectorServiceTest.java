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
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.junit.Test;
import org.terracotta.management.call.Parameter;
import org.terracotta.management.context.Context;
import org.terracotta.management.message.Message;
import org.terracotta.management.message.MessageType;
import org.terracotta.management.notification.ContextualNotification;
import org.terracotta.management.registry.MessageConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author Mathieu Carbou
 */
public class DefaultCollectorServiceTest {

  @Test(timeout = 6000)
  public void test_collector() throws Exception {
    final Queue<Message> messages = new ConcurrentLinkedQueue<Message>();
    final List<String> notifs = new ArrayList<String>(6);
    final CountDownLatch num = new CountDownLatch(5);

    CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB))
        .build();

    StatisticsProviderConfiguration statisticsProviderConfiguration = new EhcacheStatisticsProviderConfiguration(
        1, TimeUnit.MINUTES,
        100, 1, TimeUnit.SECONDS,
        2, TimeUnit.SECONDS);

    ManagementRegistryService managementRegistry = new DefaultManagementRegistryService(new DefaultManagementRegistryConfiguration()
        .addConfiguration(statisticsProviderConfiguration)
        .setCacheManagerAlias("my-cm-1"));

    CollectorService collectorService = new DefaultCollectorService(new MessageConsumer() {
      @Override
      public void accept(Message message) {
        System.out.println(message);
        messages.offer(message);
        if (message.getType().equals(MessageType.NOTIFICATION.name())) {
          notifs.add(message.unwrap(ContextualNotification.class).getType());
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
        .call("updateCollectedStatistics",
            new Parameter("StatisticsCapability"),
            new Parameter(asList("PutCounter", "InexistingRate"), Collection.class.getName()))
        .on(Context.create("cacheManagerName", "my-cm-1"))
        .build()
        .execute()
        .getSingleResult();

    managementRegistry.withCapability("StatisticCollectorCapability")
        .call("startStatisticCollector")
        .on(Context.create("cacheManagerName", "my-cm-1"))
        .build()
        .execute()
        .getSingleResult();

    Cache<String, String> cache = cacheManager.createCache("my-cache", cacheConfiguration);
    cache.put("key", "val");

    num.await();

    cacheManager.removeCache("my-cache");
    cacheManager.close();

    assertThat(notifs, equalTo(Arrays.asList("CACHE_MANAGER_AVAILABLE", "CACHE_MANAGER_CLOSED", "CACHE_MANAGER_AVAILABLE", "CACHE_ADDED", "CACHE_REMOVED", "CACHE_MANAGER_CLOSED")));
    assertThat(messages.size(), equalTo(7));
  }

}
