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

import java.util.Set;

/**
 * A collection of {@link ResourcePool resource pools} a cache has at its disposal to store its mappings.
 *
 * @author Ludovic Orban
 */
public interface ResourcePools {

  /**
   * Get a specific {@link ResourcePool} based on its type.
   *
   * @param <P> specific resource pool type
   * @param resourceType the type of resource the pool is tracking.
   * @return the {@link ResourcePool}, or null if there is no pool tracking the requested type.
   */
  <P extends ResourcePool> P getPoolForResource(ResourceType<P> resourceType);

  /**
   * Get the set of {@link ResourceType} of all the pools present in the ResourcePools
   *
   * @return the set of {@link ResourceType}
   */
  Set<ResourceType<?>> getResourceTypeSet();

  /**
   * Get a copy of the current {@link ResourcePools} merged with another {@link ResourcePools} and validate that
   * the updates to the contained {@link ResourcePool}s are legal.
   *
   * @param toBeUpdated the {@link ResourcePools} to merge with the current one.
   * @return A merged and validated {@link ResourcePools} copy.
   * @throws IllegalArgumentException      thrown when an illegal resource value is being given, for instance a negative
   *                                       size.
   * @throws UnsupportedOperationException thrown when an unsupported update is requested, for instance trying to change
   *                                       the {@link ResourceUnit}.
   */
  ResourcePools validateAndMerge(ResourcePools toBeUpdated) throws IllegalArgumentException, UnsupportedOperationException;

}
