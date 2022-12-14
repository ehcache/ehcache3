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

import org.ehcache.spi.service.Service;
import org.terracotta.connection.entity.Entity;

/**
 * Provides support for accessing server entities through the Cache Manager connection
 */
public interface EntityService extends Service {

  /**
   * Creates a factory class used to create, fetch and destroy server entities through the same connection used by this {@link org.ehcache.CacheManager}
   *
   * @param entityIdentifier The entity name
   * @param entityType       The entity class
   * @param entityVersion    The entity version
   * @param configuration    Any configuration object that might be pass when creating the entity, or null if none
   * @param <E>              The entity type, extending {@link Entity}
   * @param <C>              The configuration class
   * @return The created factory
   */
  <E extends Entity, C> ClientEntityFactory<E, C> newClientEntityFactory(String entityIdentifier, Class<E> entityType, long entityVersion, C configuration);

}
