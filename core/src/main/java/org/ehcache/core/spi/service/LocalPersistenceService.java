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

package org.ehcache.core.spi.service;

import org.ehcache.CachePersistenceException;
import org.ehcache.spi.service.PersistableResourceService;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * Service to provide persistence context to caches requiring it.
 * <P>
 * Will be used by caches with a disk store, whether or not the data should survive a program restart.
 * <P/>
 */
public interface LocalPersistenceService extends PersistableResourceService {

  /**
   * Retrieves an existing or creates a new persistence space
   *
   * @param name the name to be used for the persistence space
   *
   * @return a {@link PersistenceSpaceIdentifier}
   *
   * @throws CachePersistenceException if the persistence space cannot be created
   */
  PersistenceSpaceIdentifier getOrCreatePersistenceSpace(String name) throws CachePersistenceException;

  /**
   * Creates a new persistence context within the given space.
   *
   * @param space space to create within
   * @param name name of the context to create
   * @return a {@link FileBasedPersistenceContext}
   */
  FileBasedPersistenceContext createPersistenceContextWithin(PersistenceSpaceIdentifier space, String name) throws CachePersistenceException;

  /**
   * Destroys all persistence spaces
   *
   * Note that this method can be called without creating the persistence space beforehand in the same JVM.
   * It will nonetheless try to delete any persistent data associated with the root directory provided
   * in the service.
   */
  void destroyAll();

  /**
   * An identifier for an existing persistence space.
   */
  interface PersistenceSpaceIdentifier extends ServiceConfiguration<LocalPersistenceService> {}
}
