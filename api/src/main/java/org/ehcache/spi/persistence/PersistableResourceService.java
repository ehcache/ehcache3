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

package org.ehcache.spi.persistence;

import org.ehcache.config.ResourceType;
import org.ehcache.CachePersistenceException;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Interface for {@link org.ehcache.spi.service.Service Service}s that handle a {@link ResourceType} which is
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
   * Returns a {@link PersistenceSpaceIdentifier} for the space associated to the provided arguments.
   * <p>
   * This method may create a new persistence space or load one. The returned identifier is the only way to interact
   * with the persistence space.
   *
   * @param name the name of the persistence context
   * @param config the configuration for the associated cache
   * @throws CachePersistenceException if the persistence space cannot be created
   *
   * @return an identifier for the persistence space
   *
   * @see #getStateRepositoryWithin(PersistenceSpaceIdentifier, String)
   */
  PersistenceSpaceIdentifier<?> getPersistenceSpaceIdentifier(String name, CacheConfiguration<?, ?> config) throws CachePersistenceException;

  /**
   * Releases a previously obtained {@link PersistenceSpaceIdentifier}.
   * <p>
   * This indicates to the persistence space that resource linked to the given identifier are no longer needed and thus
   * enables cleaning up any transient state left.
   *
   * @param identifier the {@code PersistenceSpaceIdentifier} to release
   * @throws CachePersistenceException If the identifier is not known
   */
  void releasePersistenceSpaceIdentifier(PersistenceSpaceIdentifier<?> identifier) throws CachePersistenceException;

  /**
   * Returns a named {@link StateRepository state repository} in the context of the given
   * {@link PersistenceSpaceIdentifier identifier}.
   * <p>
   * If the {@code StateRepository} already existed, this method returns it in a fully available state.
   *
   * @param identifier the space identifier
   * @param name the state repository name
   * @return a {@code StateRepository}
   *
   * @throws CachePersistenceException if the {@code StateRepository} cannot be created or recovered.
   */
  StateRepository getStateRepositoryWithin(PersistenceSpaceIdentifier<?> identifier, String name) throws CachePersistenceException;

  /**
   * Destroys the persistence space with the given name.
   * <p>
   * This method can be called without having created the persistence space
   * from this JVM.
   *
   * @param name persistence context name
   *
   * @throws CachePersistenceException if the persistence space cannot be destroyed
   */
  void destroy(String name) throws CachePersistenceException;

  /**
   * Destroys all persistence spaces.
   * <p>
   * Note that this method can be called without having created the persistence
   * spaces from this JVM.
   *
   * @throws CachePersistenceException if the persistence storage cannot be destroyed
   */
  void destroyAll() throws CachePersistenceException;

  /**
   * An identifier for an existing persistable resource.
   *
   * @param <T> the associated persistence service type
   */
  interface PersistenceSpaceIdentifier<T extends PersistableResourceService> extends ServiceConfiguration<T, Void> {}
}
