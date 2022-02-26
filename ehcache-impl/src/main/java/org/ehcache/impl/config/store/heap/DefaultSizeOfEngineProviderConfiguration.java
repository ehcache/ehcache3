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

package org.ehcache.impl.config.store.heap;

import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.store.heap.SizeOfEngineProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 * {@link ServiceCreationConfiguration} for the default {@link SizeOfEngineProvider}.
 */
public class DefaultSizeOfEngineProviderConfiguration implements ServiceCreationConfiguration<SizeOfEngineProvider, DefaultSizeOfEngineProviderConfiguration> {

  private final long objectGraphSize;
  private final long maxObjectSize;
  private final MemoryUnit unit;

  /**
   * Creates a new configuration object with the provided parameters.
   *
   * @param size the maximum object size
   * @param unit the object size unit
   * @param objectGraphSize the maximum object graph size
   */
  public DefaultSizeOfEngineProviderConfiguration(long size, MemoryUnit unit, long objectGraphSize) {
    if (size <= 0 || objectGraphSize <= 0) {
      throw new IllegalArgumentException("SizeOfEngine cannot take non-positive arguments.");
    }
    this.objectGraphSize = objectGraphSize;
    this.maxObjectSize = size;
    this.unit = unit;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Class<SizeOfEngineProvider> getServiceType() {
    return SizeOfEngineProvider.class;
  }

  /**
   * Returns the maximum object graph size before aborting sizing.
   * <p>
   * This measure is a count of different instances that have to be sized from the root of the graph.
   * That is a collection with 1000 identical elements will count as two objects, while a collection with 1000
   * different elements will count as 1001 objects.
   *
   * @return the maximum graph size
   */
  public long getMaxObjectGraphSize() {
    return this.objectGraphSize;
  }

  /**
   * Returns the maximum object size before aborting sizing.
   * <p>
   * This value applies to the sum of the size of objects composing the graph being sized.
   *
   * @return the maximum object size
   *
   * @see #getUnit()
   */
  public long getMaxObjectSize() {
    return this.maxObjectSize;
  }

  /**
   * Returns the maximum object size unit.
   *
   * @return the maximum object size unit
   *
   * @see #getMaxObjectSize()
   */
  public MemoryUnit getUnit() {
    return this.unit;
  }

  @Override
  public DefaultSizeOfEngineProviderConfiguration derive() {
    return new DefaultSizeOfEngineProviderConfiguration(maxObjectSize, unit, objectGraphSize);
  }

  @Override
  public DefaultSizeOfEngineProviderConfiguration build(DefaultSizeOfEngineProviderConfiguration configuration) {
    return configuration;
  }
}
