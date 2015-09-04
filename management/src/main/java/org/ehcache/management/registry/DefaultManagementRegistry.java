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

import org.ehcache.CacheManager;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.actions.EhcacheActionProvider;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.lifecycle.LifeCycleListenerAdapter;
import org.ehcache.spi.lifecycle.LifeCycleService;
import org.ehcache.spi.alias.AliasService;
import org.ehcache.spi.service.ServiceDependencies;
import org.terracotta.context.annotations.ContextAttribute;
import org.terracotta.statistics.StatisticsManager;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ludovic Orban
 */
@ServiceDependencies({AliasService.class, LifeCycleService.class})
public class DefaultManagementRegistry extends AbstractManagementRegistry {

  public static final EhcacheStatisticsProviderConfiguration DEFAULT_EHCACHE_STATISTICS_PROVIDER_CONFIGURATION = new EhcacheStatisticsProviderConfiguration(5 * 60, TimeUnit.SECONDS, 100, 1, TimeUnit.SECONDS, 30, TimeUnit.SECONDS);
  private final DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration;
  private volatile ScheduledExecutorService executor;

  private final AtomicInteger startedCounter = new AtomicInteger();

  public DefaultManagementRegistry() {
    this(null);
  }

  public DefaultManagementRegistry(DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration) {
    this.defaultManagementRegistryConfiguration = defaultManagementRegistryConfiguration;
  }

  @Override
  public void start(final ServiceProvider serviceProvider) {

    serviceProvider.getService(LifeCycleService.class).register(CacheManager.class, new LifeCycleListenerAdapter<CacheManager>() {
      
      // must be kept as a string reference in this listener because the StatisticsManager class  is using weak references
      EhcacheManagerStatsSettings ehcacheManagerStatsSettings;
      
      @Override
      public void afterInitialization(CacheManager instance) {
        String cacheManagerName = serviceProvider.getService(AliasService.class).getCacheManagerAlias();
        ehcacheManagerStatsSettings = new EhcacheManagerStatsSettings(cacheManagerName, Collections.<String, Object>singletonMap("Setting", "CacheManagerName"));
        StatisticsManager.associate(ehcacheManagerStatsSettings).withParent(instance);
      }

      @Override
      public void afterClosing(CacheManager instance) {
        StatisticsManager.dissociate(ehcacheManagerStatsSettings).fromParent(instance);
      }
    });

    if (startedCounter.getAndIncrement() > 0) {
      return;
    }

    StatisticsProviderConfiguration statisticsProviderConfiguration;
    if (defaultManagementRegistryConfiguration == null) {
        statisticsProviderConfiguration = DEFAULT_EHCACHE_STATISTICS_PROVIDER_CONFIGURATION;
    } else {
      statisticsProviderConfiguration = defaultManagementRegistryConfiguration.getConfigurationFor(EhcacheStatisticsProvider.class);
    }

    //TODO: get this from appropriate service (see #350)
    executor = Executors.newScheduledThreadPool(1);

    addSupportFor(new EhcacheStatisticsProvider(statisticsProviderConfiguration, executor));
    addSupportFor(new EhcacheActionProvider());

    super.start(serviceProvider);
  }

  @Override
  public void stop() {
    int count = startedCounter.decrementAndGet();
    if (count < 0) {
      startedCounter.incrementAndGet();
    }
    if (count == 0) {
      super.stop();
      executor.shutdown();
      executor = null;
    }
  }

  private static final class EhcacheManagerStatsSettings {
    @ContextAttribute("CacheManagerName") private final String name;
    @ContextAttribute("properties") private final Map<String, Object> properties;
    @ContextAttribute("tags") private final Set<String> tags = new HashSet<String>(Arrays.asList("cacheManager", "exposed"));

    EhcacheManagerStatsSettings(String name, Map<String, Object> properties) {
      this.name = name;
      this.properties = properties;
    }
  }

}
