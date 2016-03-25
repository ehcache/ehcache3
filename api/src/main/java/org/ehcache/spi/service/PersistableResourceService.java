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

package org.ehcache.spi.service;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.exceptions.CachePersistenceException;

import java.util.Collection;

/**
 * Specific interface for services that are dedicated to handling a {@link ResourceType} which is
 * {@link ResourceType#isPersistable() persistable}.
 */
@PluralService
public interface PersistableResourceService extends MaintainableService {

  /**
   * Indicates whether this service handles the type of resource passed in.
   *
   * @param resourceType the resource type to handle
   * @return {@code true} if this service handles this resource type, {@code false} otherwise
   */
  boolean handlesResourceType(ResourceType<?> resourceType);

  /**
   * Enables this service to create additional configurations to enable support of the passed in resource pool in the
   * context of the given alias.
   *
   * @param alias the alias context
   * @param pool the resource pool
   * @return a {@link Collection} of {@link ServiceConfiguration}, can be empty
   * @throws CachePersistenceException in case of a persistence related problem
   * @throws IllegalArgumentException if {@code handlesResourceType(pool.getType) != true}
   */
  Collection<ServiceConfiguration<?>> additionalConfigurationsForPool(String alias, ResourcePool pool) throws CachePersistenceException;

  /**
   * Destroys the persistence space with the given name.
   *
   * Note that this method can be called without creating the persistence space beforehand in the same JVM.
   * It will nonetheless try to delete any persistent data that could have been associated with the name.
   *
   * @param name the name of the persistence context
   *
   * @throws CachePersistenceException if the persistence space cannot be destroyed
   */
  void destroy(String name) throws CachePersistenceException;

  /**
   * Creates the persistent storage.
   */
  void create();

  /**
   * Destroys all persistence spaces
   *
   * Note that this method can be called without creating the persistence space beforehand in the same JVM.
   * It will nonetheless try to delete any persistent data associated with the base configuration provided
   * in the service.
   */
  void destroyAll();
}
