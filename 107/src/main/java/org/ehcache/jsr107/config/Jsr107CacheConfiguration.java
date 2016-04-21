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

import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Jsr107CacheConfiguration
 */
public class Jsr107CacheConfiguration implements ServiceConfiguration<Jsr107Service> {

  private final boolean statisticsEnabled;
  private final boolean managementEnabled;

  public Jsr107CacheConfiguration(boolean statisticsEnabled, boolean managementEnabled) {

    this.statisticsEnabled = statisticsEnabled;
    this.managementEnabled = managementEnabled;
  }

  @Override
  public Class<Jsr107Service> getServiceType() {
    return Jsr107Service.class;
  }

  public boolean isManagementEnabled() {
    return managementEnabled;
  }

  public boolean isStatisticsEnabled() {
    return statisticsEnabled;
  }
}
