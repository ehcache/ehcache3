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

package org.ehcache.impl.config;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.core.HumanReadable;

/**
 * Implementation of the {@link SizedResourcePool} interface.
 *
 * @param <P> resource pool type
 */
public class SizedResourcePoolImpl<P extends SizedResourcePool> extends AbstractResourcePool<P, ResourceType<P>>
    implements SizedResourcePool, HumanReadable {

  private final long size;
  private final ResourceUnit unit;

  /**
   * Creates a new resource pool based on the provided parameters.
   *
   * @param type the resource type
   * @param size the size
   * @param unit the unit for the size
   * @param persistent whether the pool is to be persistent
   */
  public SizedResourcePoolImpl(ResourceType<P> type, long size, ResourceUnit unit, boolean persistent) {
    super(type, persistent);
    if (unit == null) {
      throw new NullPointerException("ResourceUnit can not be null");
    }
    if (size <= 0) {
      throw new IllegalArgumentException("Size must be greater than 0");
    }
    if (!type.isPersistable() && persistent) {
      throw new IllegalStateException("Non-persistable resource cannot be configured persistent");
    }
    this.size = size;
    this.unit = unit;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public long getSize() {
    return size;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public ResourceUnit getUnit() {
    return unit;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException {@inheritDoc}
   */
  @Override
  public void validateUpdate(final ResourcePool newPool) {
    super.validateUpdate(newPool);

    SizedResourcePool sizedPool = (SizedResourcePool)newPool;
    // Ensure unit type has not changed
    if (!this.getUnit().getClass().equals(sizedPool.getUnit().getClass())) {
      throw new IllegalArgumentException("ResourcePool for " + sizedPool.getType() + " with ResourceUnit '"
          + sizedPool.getUnit() + "' can not replace '" + this.getUnit() + "'");
    }

    // Ensure replacement has positive space
    if (sizedPool.getSize() <= 0) {
      throw new IllegalArgumentException("ResourcePool for " + sizedPool.getType()
          + " must specify space greater than 0");
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "Pool {" + getSize() + " " + getUnit() + " " + getType() + (isPersistent() ? "(persistent)}" : "}");
  }

  @Override
  public String readableString() {
    return getSize() + " " + getUnit() + " " + (isPersistent() ? "(persistent)" : "");
  }
}
