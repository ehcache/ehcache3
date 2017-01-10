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
package org.ehcache.management.providers;

import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.registry.collect.StatisticCollector;
import org.terracotta.management.registry.collect.StatisticCollectorProvider;

@RequiredContext(@Named("cacheManagerName"))
public class EhcacheStatisticCollectorProvider extends StatisticCollectorProvider<StatisticCollector> {
  public EhcacheStatisticCollectorProvider(ManagementRegistryServiceConfiguration configuration) {
    super(StatisticCollector.class, configuration.getContext());
  }
}
