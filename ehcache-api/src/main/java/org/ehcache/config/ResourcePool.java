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
package org.ehcache.config;

/**
 * A resource providing capacity to be used by {@link org.ehcache.Cache Cache}s.
 * <p>
 * <em>Implementations must be immutable.</em>
 */
public interface ResourcePool {

  /**
   * Get the {@link ResourceType}.
   *
   * @return the resource type
   */
  ResourceType<?> getType();

  /**
   * Indicates whether the underlying resource is persistent.
   * <p>
   * Persistence in this context means that data stored will survive a JVM restart, unless destroyed.
   *
   * @return {@code true} if persistent, {@code false} otherwise
   */
  boolean isPersistent();

  /**
   * Validates whether or not a new {@code ResourcePool} can replace this {@code ResourcePool} on a running
   * {@link org.ehcache.Cache Cache}.
   *
   * @param newPool the pool which is the candidate for replacing this {@code ResourcePool}
   *
   * @throws IllegalArgumentException if {@code newPool} is not a valid replacement for this {@code ResourcePool}
   */
  void validateUpdate(ResourcePool newPool);

}
