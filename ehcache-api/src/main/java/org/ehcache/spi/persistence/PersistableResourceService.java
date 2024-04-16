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

import org.ehcache.spi.service.MaintainableService;
import org.ehcache.spi.service.PluralService;

/**
 * Interface for {@link org.ehcache.spi.service.Service Service}s that handle a {@link ResourceType} which is
 * {@link ResourceType#isPersistable() persistable}.
 */
@PluralService
public interface PersistableResourceService extends PersistableIdentityService, MaintainableService {

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
}
