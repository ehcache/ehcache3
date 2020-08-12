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
package org.ehcache.clustered.client.internal.service;

import org.ehcache.clustered.client.service.ClientEntityFactory;
import org.ehcache.clustered.client.service.EntityBusyException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.connection.Connection;
import org.terracotta.connection.entity.Entity;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.exception.EntityAlreadyExistsException;
import org.terracotta.exception.EntityConfigurationException;
import org.terracotta.exception.EntityNotFoundException;
import org.terracotta.exception.EntityNotProvidedException;
import org.terracotta.exception.EntityVersionMismatchException;
import org.terracotta.exception.PermanentEntityException;

abstract class AbstractClientEntityFactory<E extends Entity, C, U> implements ClientEntityFactory<E, C> {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractClientEntityFactory.class);

  private final String entityIdentifier;
  private final Class<E> entityType;
  private final long entityVersion;
  private final C configuration;

  /**
   * @param entityIdentifier the instance identifier for the {@code {@link Entity}}
   * @param configuration    the {@code {@link Entity}} configuration
   */
  AbstractClientEntityFactory(String entityIdentifier, Class<E> entityType, long entityVersion, C configuration) {
    this.entityIdentifier = entityIdentifier;
    this.entityType = entityType;
    this.entityVersion = entityVersion;
    this.configuration = configuration;
  }

  @Override
  public C getConfiguration() {
    return configuration;
  }

  @Override
  public String getEntityIdentifier() {
    return entityIdentifier;
  }

  @Override
  public Class<E> getEntityType() {
    return entityType;
  }

  @Override
  public long getEntityVersion() {
    return entityVersion;
  }

  @Override
  public void create() throws EntityAlreadyExistsException {
    EntityRef<E, C, U> ref = getEntityRef();
    try {
      while (true) {
        ref.create(configuration);
        try {
          ref.fetchEntity(null).close();
          return;
        } catch (EntityNotFoundException e) {
          //continue;
        }
      }
    } catch (EntityNotProvidedException | EntityVersionMismatchException e) {
      LOGGER.error("Unable to create entity {} for id {}", entityType.getName(), entityIdentifier, e);
      throw new AssertionError(e);
    } catch (EntityConfigurationException e) {
      LOGGER.error("Unable to create entity - configuration exception", e);
      throw new AssertionError(e);
    }
  }

  @Override
  public E retrieve() throws EntityNotFoundException {
    try {
      return getEntityRef().fetchEntity(null);
    } catch (EntityVersionMismatchException e) {
      LOGGER.error("Unable to retrieve entity {} for id {}", entityType.getName(), entityIdentifier, e);
      throw new AssertionError(e);
    }
  }

  @Override
  public void destroy() throws EntityNotFoundException, EntityBusyException {
    EntityRef<E, C, U> ref = getEntityRef();
    try {
      if (!ref.destroy()) {
        throw new EntityBusyException("Destroy operation failed; " + entityIdentifier + " cluster tier in use by other clients");
      }
    } catch (EntityNotProvidedException e) {
      LOGGER.error("Unable to destroy entity {} for id {}", entityType.getName(), entityIdentifier, e);
      throw new AssertionError(e);
    } catch (PermanentEntityException e) {
      LOGGER.error("Unable to destroy entity - server says it is permanent", e);
      throw new AssertionError(e);
    }
  }

  private EntityRef<E, C, U> getEntityRef() {
    try {
      return getConnection().getEntityRef(entityType, entityVersion, entityIdentifier);
    } catch (EntityNotProvidedException e) {
      LOGGER.error("Unable to find reference for entity {} for id {}", entityType.getName(), entityIdentifier, e);
      throw new AssertionError(e);
    }
  }

  protected abstract Connection getConnection();

}
