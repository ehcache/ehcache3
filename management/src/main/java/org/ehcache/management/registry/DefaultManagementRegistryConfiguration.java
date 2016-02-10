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

import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.terracotta.management.context.Context;
import org.terracotta.management.registry.ManagementProvider;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Ludovic Orban
 */
public class DefaultManagementRegistryConfiguration implements ManagementRegistryServiceConfiguration {

  private static final AtomicLong COUNTER = new AtomicLong();

  private final Map<Class<? extends ManagementProvider>, StatisticsProviderConfiguration<?>> configurationMap = new HashMap<Class<? extends ManagementProvider>, StatisticsProviderConfiguration<?>>();
  private Context context = Context.empty();
  private String statisticsExecutorAlias;
  private String collectorExecutorAlias;

  public DefaultManagementRegistryConfiguration() {
    setCacheManagerAlias("cache-manager-" + COUNTER.getAndIncrement());
    addConfiguration(new EhcacheStatisticsProviderConfiguration(1, TimeUnit.MINUTES, 100, 1, TimeUnit.SECONDS, 30, TimeUnit.SECONDS));
  }

  public DefaultManagementRegistryConfiguration setCacheManagerAlias(String alias) {
    return setContext(Context.create("cacheManagerName", alias));
  }

  public DefaultManagementRegistryConfiguration setContext(Context context) {
    if (!this.context.contains("cacheManagerName") && !context.contains("cacheManagerName")) {
      throw new IllegalArgumentException("'cacheManagerName' is missing from context");
    }
    this.context = this.context.with(context);
    return this;
  }

  public DefaultManagementRegistryConfiguration setStatisticsExecutorAlias(String alias) {
    this.statisticsExecutorAlias = alias;
    return this;
  }

  public DefaultManagementRegistryConfiguration setCollectorExecutorAlias(String collectorExecutorAlias) {
    this.collectorExecutorAlias = collectorExecutorAlias;
    return this;
  }

  public DefaultManagementRegistryConfiguration addConfiguration(StatisticsProviderConfiguration<?> configuration) {
    Class<? extends ManagementProvider> serviceType = configuration.getStatisticsProviderType();
    configurationMap.put(serviceType, configuration);
    return this;
  }

  @Override
  public Context getContext() {
    return context;
  }

  @Override
  public String getStatisticsExecutorAlias() {
    return this.statisticsExecutorAlias;
  }

  @Override
  public String getCollectorExecutorAlias() {
    return this.collectorExecutorAlias;
  }

  @Override
  public StatisticsProviderConfiguration getConfigurationFor(Class<? extends ManagementProvider<?>> managementProviderClass) {
    return configurationMap.get(managementProviderClass);
  }

  @Override
  public Class<ManagementRegistryService> getServiceType() {
    return ManagementRegistryService.class;
  }

}
