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

import org.ehcache.jsr107.Jsr107Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Jsr107CacheConfiguration
 */
public class Jsr107CacheConfiguration implements ServiceConfiguration<Jsr107Service, Void> {

  private final ConfigurationElementState statisticsEnabled;
  private final ConfigurationElementState managementEnabled;

  public Jsr107CacheConfiguration(ConfigurationElementState statisticsEnabled, ConfigurationElementState managementEnabled) {
    this.statisticsEnabled = statisticsEnabled;
    this.managementEnabled = managementEnabled;
  }

  @Override
  public Class<Jsr107Service> getServiceType() {
    return Jsr107Service.class;
  }

  /**
   * Indicates if management is to be enabled.
   * <p>
   * A {@code null} return value indicates that cache manager level config is to be taken.
   *
   * @return {@code true} if management is enabled, {@code false} if disabled, {@code null} to keep cache manager level
   */
  public ConfigurationElementState isManagementEnabled() {
    return managementEnabled;
  }

  /**
   * Indicates if statistics are to be enabled.
   * <p>
   * A {@code null} return value indicates that cache manager level config is to be taken.
   *
   * @return {@code true} if statistics are enabled, {@code false} if disabled, {@code null} to keep cache manager level
   */
  public ConfigurationElementState isStatisticsEnabled() {
    return statisticsEnabled;
  }
}
