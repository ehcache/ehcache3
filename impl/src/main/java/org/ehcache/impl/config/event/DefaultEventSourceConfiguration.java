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

package org.ehcache.impl.config.event;

import org.ehcache.core.config.store.StoreEventSourceConfiguration;
import org.ehcache.core.spi.store.Store;

/**
 * {@link org.ehcache.spi.service.ServiceConfiguration} for a {@link org.ehcache.core.spi.store.Store.Provider}
 * related to {@link org.ehcache.core.spi.store.events.StoreEvent}s.
 */
public class DefaultEventSourceConfiguration implements StoreEventSourceConfiguration {

  private final int orderedEventParallelism;

  /**
   * Creates a new configuration with the provided level of parallelism for ordered events.
   *
   * @param orderedEventParallelism  the parallelism level for ordered events
   */
  public DefaultEventSourceConfiguration(int orderedEventParallelism) {
    if (orderedEventParallelism <= 0) {
      throw new IllegalArgumentException("Event parallelism must be a value bigger than 0");
    }
    this.orderedEventParallelism = orderedEventParallelism;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int getOrderedEventParallelism() {
    return orderedEventParallelism;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<Store.Provider> getServiceType() {
    return Store.Provider.class;
  }
}
