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

import org.ehcache.management.ManagementRegistry;
import org.ehcache.management.ManagementRegistryConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.ManagementProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class DefaultManagementRegistryConfiguration implements ManagementRegistryConfiguration {

  private final Map<Class<? extends ManagementProvider>, StatisticsProviderConfiguration<?>> configurationMap = new HashMap<Class<? extends ManagementProvider>, StatisticsProviderConfiguration<?>>();

  public ManagementRegistryConfiguration addConfiguration(StatisticsProviderConfiguration<?> configuration) {
    Class<? extends ManagementProvider> serviceType = configuration.getStatisticsProviderType();
    configurationMap.put(serviceType, configuration);
    return this;
  }

  @Override
  public StatisticsProviderConfiguration getConfigurationFor(Class<? extends ManagementProvider<?>> managementProviderClass) {
    return configurationMap.get(managementProviderClass);
  }

  @Override
  public Class<ManagementRegistry> getServiceType() {
    return ManagementRegistry.class;
  }
}
