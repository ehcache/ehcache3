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

import org.ehcache.core.statistics.LatencyHistogramConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.terracotta.management.model.context.Context;

import java.util.Collection;

/**
 * Configuration interface for a  {@link ManagementRegistryService}.
 */
public interface ManagementRegistryServiceConfiguration extends ServiceCreationConfiguration<ManagementRegistryService, Void> {

  /**
   * The context used to identify this cache manager
   */
  Context getContext();

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
   * @return an identifier used to identify this running instance. It will be the same even if a clustered Ehcache client reconnects (and clientId changes).
   */
  String getInstanceId();

  /**
   * Configuration of the latency histogram derived property. It is used to setup
   * different resolution parameters of the histogram.
   *
   * @return configuration of the latency histogram
   */
  LatencyHistogramConfiguration getLatencyHistogramConfiguration();
}
