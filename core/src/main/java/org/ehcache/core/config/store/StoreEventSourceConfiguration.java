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
 * {@link ServiceConfiguration} used by the {@link org.ehcache.core.EhcacheManager} to populate the dispatcher
 * concurrency in the {@link StoreConfigurationImpl}.
 */
public interface StoreEventSourceConfiguration extends ServiceConfiguration<Store.Provider> {

  /**
   * Default dispatcher concurrency
   */
  int DEFAULT_DISPATCHER_CONCURRENCY = 1;

  /**
   * Indicates over how many buckets should ordered events be spread
   *
   * @return the number of buckets to use
   */
  int getDispatcherConcurrency();
}
