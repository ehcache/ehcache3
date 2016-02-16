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
package org.ehcache.impl.internal.sizeof;

import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.sizeof.SizeOfEngineConfiguration;
import org.ehcache.core.spi.sizeof.SizeOfEngineProvider;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngineConfiguration implements SizeOfEngineConfiguration {

  public static final int DEFAULT_OBJECT_GRAPH_SIZE = 1000;
  public static final long DEFAULT_MAX_OBJECT_SIZE = Long.MAX_VALUE;
  public static final MemoryUnit DEFAULT_UNIT = MemoryUnit.B;

  private final long objectGraphSize;
  private final long maxObjectSize;
  private final MemoryUnit unit;

  public DefaultSizeOfEngineConfiguration(long size, MemoryUnit unit, long objectGraphSize) {
    if (size <= 0 || objectGraphSize <= 0) {
      throw new IllegalArgumentException("ObjectGraphSize/ObjectSize can only accept positive values.");
    }
    this.objectGraphSize = objectGraphSize;
    this.maxObjectSize = size;
    this.unit = unit;
  }

  @Override
  public Class<SizeOfEngineProvider> getServiceType() {
    return SizeOfEngineProvider.class;
  }

  @Override
  public long getMaxObjectGraphSize() {
    return this.objectGraphSize;
  }

  @Override
  public long getMaxObjectSize() {
    return this.maxObjectSize;
  }

  @Override
  public MemoryUnit getUnit() {
    return this.unit;
  }

}
