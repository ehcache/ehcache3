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

/**
 * Foundation implementation for {@link ResourcePool} implementations.
 *
 * @param <P> the type of {@link ResourcePool} implemented by the subclass
 * @param <T> the type of {@link ResourceType} related to the resource pool
 */
public abstract class AbstractResourcePool<P extends ResourcePool, T extends ResourceType<P>> implements ResourcePool {
  private final T type;
  private final boolean persistent;

  /**
   * Creates a {@code AbstractResourcePool} instance.
   *
   * @param type the non-{@code null} {@code ResourceType}
   * @param persistent whether or not this {@code ResourcePool} is persistent
   */
  protected AbstractResourcePool(T type, boolean persistent) {
    if (type == null) {
      throw new NullPointerException("ResourceType may not be null");
    }
    this.type = type;
    this.persistent = persistent;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public T getType() {
    return type;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isPersistent() {
    return persistent;
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException {@inheritDoc}
   */
  @Override
  public void validateUpdate(ResourcePool newPool) {
    // Replacement must be of the same ResourceType
    if (!this.getType().equals(newPool.getType())) {
      throw new IllegalArgumentException("ResourceType " + newPool.getType() + " can not replace " + this.getType());
    }
    // Replacement must have the same persistence
    if (this.isPersistent() != newPool.isPersistent()) {
      throw new IllegalArgumentException("ResourcePool for " + newPool.getType() + " with isPersistent="
          + newPool.isPersistent() + " can not replace isPersistent=" + this.isPersistent());
    }
  }
}
