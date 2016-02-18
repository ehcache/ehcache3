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

package org.ehcache.impl.config.sizeof;

import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.sizeof.SizeOfEngineProviderConfiguration;
import org.ehcache.core.spi.sizeof.SizeOfEngineProvider;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngineProviderConfiguration implements SizeOfEngineProviderConfiguration {

  private final long objectGraphSize;
  private final long maxObjectSize;
  private final MemoryUnit unit;

  public DefaultSizeOfEngineProviderConfiguration(long size, MemoryUnit unit, long objectGraphSize) {
    if (size <= 0 || objectGraphSize <= 0) {
      throw new IllegalArgumentException("SizeOfEngine cannot take non-positive arguments.");
    }
    this.objectGraphSize = objectGraphSize;
    this.maxObjectSize = size;
    this.unit = unit;
  }

  @Override
  public Class<SizeOfEngineProvider> getServiceType() {
    return SizeOfEngineProvider.class;
  }

  public long getMaxObjectGraphSize() {
    return this.objectGraphSize;
  }

  public long getMaxObjectSize() {
    return this.maxObjectSize;
  }

  @Override
  public MemoryUnit getUnit() {
    return this.unit;
  }
}
