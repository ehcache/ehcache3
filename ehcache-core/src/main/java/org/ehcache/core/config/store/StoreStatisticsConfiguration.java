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
package org.ehcache.core.config.store;

import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Configure if statistics are unable on stores. By default they are enabled in a tiered
 * configuration to accurately track the usage of each tier. If a store is
 * standing alone, then they will be disabled by default since they are a mirror
 * of the cache statistics.
 * <p>
 * Note that statistics about the store size, mapping and so on are not affected
 * by this configuration. Only operation statistics (e.g. get/put counts) are disabled.
 */
public class StoreStatisticsConfiguration implements ServiceConfiguration<Store.Provider, Boolean> {

  private final boolean operationStatisticsEnabled;

  public StoreStatisticsConfiguration(boolean operationStatisticsEnabled) {
    this.operationStatisticsEnabled = operationStatisticsEnabled;
  }

  public boolean isOperationStatisticsEnabled() {
    return operationStatisticsEnabled;
  }

  @Override
  public Class<Store.Provider> getServiceType() {
    return Store.Provider.class;
  }

  @Override
  public Boolean derive() {
    return isOperationStatisticsEnabled();
  }

  @Override
  public StoreStatisticsConfiguration build(Boolean enabled) {
    return new StoreStatisticsConfiguration(enabled);
  }
}
