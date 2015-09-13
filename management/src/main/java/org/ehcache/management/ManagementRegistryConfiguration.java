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
package org.ehcache.management;

import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.ManagementProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * Configuration interface for a  {@link ManagementRegistry}.
 *
 * @author Mathieu Carbou
 */
public interface ManagementRegistryConfiguration extends ServiceCreationConfiguration<ManagementRegistry> {

  /**
   * Gets the alias to use for the cache manager when using the {@link SharedManagementService}.
   *
   * @return The cache manager alias that will be used in the context to call methods from {@link SharedManagementService}  
   */
  String getCacheManagerAlias();

  /**
   * Returns the configuration of a specific {@link ManagementProvider} type.
   *
   * @param managementProviderClass The type of the class managing statistics, capabilities, actions, etc.
   * @return The configuration class to use for this manager type
   */
  StatisticsProviderConfiguration getConfigurationFor(Class<? extends ManagementProvider<?>> managementProviderClass);
}
