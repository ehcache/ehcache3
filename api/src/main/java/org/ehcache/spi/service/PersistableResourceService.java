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
import org.ehcache.CachePersistenceException;

import java.util.Collection;

/**
 * Interface for {@link Service}s that handle a {@link ResourceType} which is
 * {@link ResourceType#isPersistable() persistable}.
 */
@PluralService
public interface PersistableResourceService extends MaintainableService {

  /**
   * Returns {@code true} if this service handles the given resource type.
   *
   * @param resourceType the resource type to check
   * @return {@code true} if this service handles the resource type
   */
  boolean handlesResourceType(ResourceType<?> resourceType);

  /**
   * Enables this service to add configurations to support the resource pool.
   *
   * @param alias the alias context
   * @param pool the resource pool
   * @return a {@link Collection} of {@link ServiceConfiguration}
   * @throws CachePersistenceException in case of a persistence related problem
   * @throws IllegalArgumentException if {@code handlesResourceType(pool.getType)} is {@code false}
   */
  Collection<ServiceConfiguration<?>> additionalConfigurationsForPool(String alias, ResourcePool pool) throws CachePersistenceException;

  /**
   * Destroys the persistence space with the given name.
   * <P>
   *   This method can be called without having created the persistence space
   *   from this JVM.
   * </P>
   *
   * @param name persistence context name
   *
   * @throws CachePersistenceException if the persistence space cannot be destroyed
   */
  void destroy(String name) throws CachePersistenceException;

  /**
   * Creates the persistent storage.
   *
   * @throws CachePersistenceException if the persistence storage cannot be created
   */
  void create() throws CachePersistenceException;

  /**
   * Destroys all persistence spaces.
   * <P>
   * Note that this method can be called without having created the persistence
   * spaces from this JVM.
   * </P>
   *
   * @throws CachePersistenceException if the persistence storage cannot be destroyed
   */
  void destroyAll() throws CachePersistenceException;
}
