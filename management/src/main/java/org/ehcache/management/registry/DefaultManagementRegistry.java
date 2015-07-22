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

import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.actions.EhcacheActionProvider;
import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;
import org.ehcache.spi.ServiceProvider;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Ludovic Orban
 */
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
  public void start(ServiceProvider serviceProvider) {
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

}
