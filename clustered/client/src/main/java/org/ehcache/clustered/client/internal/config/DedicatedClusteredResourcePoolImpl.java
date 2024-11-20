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

package org.ehcache.clustered.client.internal.config;

import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.common.PoolAllocation;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.HumanReadable;
import org.ehcache.impl.config.SizedResourcePoolImpl;

import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;

/**
 * Concrete implementation of a {@link DedicatedClusteredResourcePool}.
 */
public class DedicatedClusteredResourcePoolImpl extends SizedResourcePoolImpl<DedicatedClusteredResourcePool>
    implements DedicatedClusteredResourcePool, HumanReadable {

  private final String fromResource;

  /**
   * Creates a new resource pool based on the provided parameters.
   *
   * @param fromResource the name of the server-based resource from which this dedicated resource pool
   *                     is reserved; may be {@code null}
   * @param size       the size
   * @param unit       the unit for the size
   */
  public DedicatedClusteredResourcePoolImpl(final String fromResource, final long size, final MemoryUnit unit) {
    super(ClusteredResourceType.Types.DEDICATED, size, unit, true);
    this.fromResource = fromResource;       // May be null
  }

  @Override
  public ClusteredResourceType<DedicatedClusteredResourcePool> getType() {
    return (ClusteredResourceType<DedicatedClusteredResourcePool>)super.getType();
  }

  @Override
  public MemoryUnit getUnit() {
    return (MemoryUnit)super.getUnit();
  }

  @Override
  public String getFromResource() {
    return this.fromResource;
  }

  @Override
  public PoolAllocation getPoolAllocation() {
    return new PoolAllocation.Dedicated(this.getFromResource(), this.getUnit().toBytes(this.getSize()));
  }

  @Override
  public void validateUpdate(final ResourcePool newPool) {
    throw new UnsupportedOperationException("Updating CLUSTERED resource is not supported");
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("Pool {");

    sb.append(getSize());
    sb.append(' ');
    sb.append(getUnit());
    sb.append(' ');
    sb.append(getType());
    sb.append(' ');
    sb.append("from=");
    if (getFromResource() == null) {
      sb.append("N/A");
    } else {
      sb.append('\'').append(getFromResource()).append('\'');
    }

    sb.append('}');
    return sb.toString();
  }

  @Override
  public String readableString() {
    return toString();
  }
}
