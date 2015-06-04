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

import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.spi.cache.Store;

/**
 * Service to provide persistence context to caches requiring it.
 *
 * Will be used by caches with a disk store, whether or not the data should survive a program restart.
 *
 * @author Alex Snaps
 */
public interface LocalPersistenceService extends Service {

  /**
   * Creates a new persistence context
   *
   * @param identifier the identifier to be used for the persistence context
   * @param storeConfiguration the configuration of the {@link Store} that will use the persistence context
   *
   * @return a {@link FileBasedPersistenceContext}
   *
   * @throws CachePersistenceException if the persistence context cannot be created
   */
  FileBasedPersistenceContext createPersistenceContext(Object identifier, Store.PersistentStoreConfiguration<?, ?, ?> storeConfiguration) throws CachePersistenceException;

  /**
   * Destroys the persistence context with the given identifier.
   *
   * Note that this method can be called without creating the persistence context beforehand in the same JVM.
   * It will nonetheless try to delete any persistent data that could have been associated with the identifier.
   *
   * @param identifier the identifier of the persistence context
   *
   * @throws CachePersistenceException if the persistence context cannot be destroyed
   */
  void destroyPersistenceContext(Object identifier) throws CachePersistenceException;

}
