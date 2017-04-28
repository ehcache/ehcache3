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

package org.ehcache.jsr107.config;

import org.ehcache.spi.service.Service;

/**
 * {@link Service} interface for JSR-107 integration.
 */
public interface Jsr107Service extends Service {

  /**
   * Returns a template name matching the provided cache alias, if any is configured.
   *
   * @param cacheAlias the cache alias
   * @return the template name or {@code null} if none configured.
   */
  String getTemplateNameForCache(String cacheAlias);

  /**
   * Indicates the loader writer behavior in atomic methods.
   * <p>
   * If {@code true} then loader writer will <em>NOT</em> be used in atomic methods, if {@code false} it will be.
   *
   * @return {@code true} or {@code false} depending on configuration
   */
  boolean jsr107CompliantAtomics();

  /**
   * Indicates if all created caches should have management enabled.
   *
   * @return {@code true} to enable management on all caches, {@code false} otherwise
   */
  ConfigurationElementState isManagementEnabledOnAllCaches();

  /**
   * Indicates if all created caches should have statistics enabled.
   *
   * @return {@code true} to enable management on all caches, {@code false} otherwise
   */
  ConfigurationElementState isStatisticsEnabledOnAllCaches();

}
