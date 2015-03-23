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
 * A collection of {@link ResourcePool resource pools} a cache has at its disposal to store its mappings.
 *
 * @author Ludovic Orban
 */
public interface ResourcePools {

  /**
   * Get a specific {@link ResourcePool} based on its type.
   *
   * @param resourceType the type of resource the pool is tracking.
   * @return the {@link ResourcePool}, or null if there is no pool tracking the requested type.
   */
  ResourcePool getPoolForResource(ResourceType resourceType);

}
