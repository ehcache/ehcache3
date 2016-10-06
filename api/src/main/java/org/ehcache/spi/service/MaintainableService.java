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

package org.ehcache.spi.service;

/**
 * {@link Service} interface that indicates that implementing services participate in
 * {@link org.ehcache.Status#MAINTENANCE MAINTENANCE} mode.
 */
@PluralService
public interface MaintainableService extends Service {

  /**
   * Defines Maintenance scope
   */
  enum MaintenanceScope {
    /** Will impact the cache manager */
    CACHE_MANAGER,
    /** Will impact one or many caches */
    CACHE
  }

  /**
   * Start this service for maintenance, based on its default configuration.
   * @param serviceProvider enables to depend on other maintainable services
   * @param maintenanceScope the scope of the maintenance
   *
   */
  void startForMaintenance(ServiceProvider<? super MaintainableService> serviceProvider, MaintenanceScope maintenanceScope);

}
