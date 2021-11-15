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
package org.ehcache.clustered.client.service;

import org.terracotta.connection.entity.Entity;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityNotFoundException;

/**
 * Factory used to create, fetch and destroy server entities.
 * <p>
 * Such factory is created with {@link EntityService#newClientEntityFactory(String, Class, long, Object)}
 */
public interface ClientEntityFactory<E extends Entity, C> {

  /**
   * @return The server entity name set by this factory
   */
  String getEntityIdentifier();

  /**
   * @return The entity type created by this factory
   */
  Class<E> getEntityType();

  /**
   * @return The entity version
   */
  long getEntityVersion();

  /**
   * @return The optional configuration object passed to the factory for entity creation, or null
   */
  C getConfiguration();

  /**
   * Creates the entity and validate that it can be effectively fetched
   *
   * @throws EntityAlreadyExistsException If the entity has already been created
   */
  void create() throws EntityAlreadyExistsException;

  /**
   * @return The created entity matching the type and identifier of this factory
   * @throws EntityNotFoundException If the entity has not been created yet
   */
  E retrieve() throws EntityNotFoundException;

  /**
   * Destroy the entity matching the factory entity identifier and type
   *
   * @throws EntityNotFoundException If the entity does not exist on the server
   * @throws EntityBusyException     If the entity is used and thus cannot be destroyed
   */
  void destroy() throws EntityNotFoundException, EntityBusyException;

}
