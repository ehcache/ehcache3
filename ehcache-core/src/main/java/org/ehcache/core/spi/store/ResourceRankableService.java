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
package org.ehcache.core.spi.store;

import org.ehcache.config.ResourceType;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Set;

@PluralService
public interface ResourceRankableService extends Service {

  /**
   * Gets the internal ranking for the instances provided by this {@code Service}
   * ability to handle the specified resources.  A higher rank value indicates a more preferable instance.
   *
   * @param resourceTypes the set of {@code ResourceType}s for the store to handle
   * @param serviceConfigs the collection of {@code ServiceConfiguration} instances that may contribute
   *                       to the ranking
   *
   * @return a non-negative rank indicating the ability of a instance created by this {@code Service}
   *      to handle the resource types specified by {@code resourceTypes}; a rank of 0 indicates an
   *      inability to handle all types specified in {@code resourceTypes}
   */
  int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs);
}
