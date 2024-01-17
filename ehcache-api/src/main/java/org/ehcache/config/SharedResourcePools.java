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

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * A collection of {@link ResourcePool resource pools} that into which one or more {@link org.ehcache.Cache Cache}s can
 * store their mappings.
 * <p>
 * <em>Implementations must be immutable.</em>
 */
public interface SharedResourcePools {

  /**
   * Gets the set of {@link ResourceType}s present in the {@code ResourcePools}.
   *
   * @return the set of {@link ResourceType}
   */
  Set<ResourceType<?>> getResourceTypeSet();

  /**
   * Get a copy of this {@code ResourcePools} merged with the given {@code ResourcePools}, validating that
   * the updates to the contained {@link ResourcePool}s are legal.
   *
   * @param toBeUpdated the {@code ResourcePools} to merge with the current one.
   * @return a validated and merged {@code ResourcePools}
   * @throws IllegalArgumentException      thrown when an illegal resource value is being given
   * @throws UnsupportedOperationException thrown when an unsupported update is requested
   */
  SharedResourcePools validateAndMerge(SharedResourcePools toBeUpdated) throws IllegalArgumentException, UnsupportedOperationException;

  Map<ResourceType<?>, ResourcePool> getSharedResourcePools ();

  Collection<ResourcePool> getResourcePools ();
}
