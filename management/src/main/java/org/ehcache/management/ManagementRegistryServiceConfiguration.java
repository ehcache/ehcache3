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
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.ManagementProvider;

import java.util.Collection;

/**
 * Configuration interface for a  {@link ManagementRegistryService}.
 */
public interface ManagementRegistryServiceConfiguration extends ServiceCreationConfiguration<ManagementRegistryService> {

  /**
   * The context used to identify this cache manager
   */
  Context getContext();

  /**
   * Gets the alias of the executor to use for asynchronous statistics tasks.
   *
   * @return The static executor alias
   */
  String getStatisticsExecutorAlias();

  /**
   * Gets the alias of the executor to use for asynchronous collector service tasks.
   *
   * @return The static colector executor alias
   */
  String getCollectorExecutorAlias();

  /**
   * The users tags that can be used to filter this client's management registry amongst others
   */
  Collection<String> getTags();

  /**
   * Returns the configuration of a specific {@link ManagementProvider} type.
   *
   * @param managementProviderClass The type of the class managing statistics, capabilities, actions, etc.
   * @return The configuration class to use for this manager type
   */
  StatisticsProviderConfiguration getConfigurationFor(Class<? extends ManagementProvider> managementProviderClass);
}
