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

package org.ehcache.impl.config.persistence;

import org.ehcache.PersistentUserManagedCache;
import org.ehcache.UserManagedCache;
import org.ehcache.config.builders.UserManagedCacheConfiguration;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.core.spi.service.LocalPersistenceService;

/**
 * Convenience configuration type that enables the {@link UserManagedCacheBuilder} to return a more specific type of
 * {@link UserManagedCache}, that is a {@link PersistentUserManagedCache}.
 */
public class UserManagedPersistenceContext<K, V> implements UserManagedCacheConfiguration<K, V, PersistentUserManagedCache<K, V>> {

  private final String identifier;
  private final LocalPersistenceService persistenceService;

  /**
   * Creates a new configuration with the provided parameters.
   *
   * @param identifier the identifier of the cache for the persistence service
   * @param persistenceService the local persistence service to use
   */
  public UserManagedPersistenceContext(String identifier, LocalPersistenceService persistenceService) {
    this.identifier = identifier;
    this.persistenceService = persistenceService;
  }

  /**
   * Transforms the builder received in one that returns a {@link PersistentUserManagedCache}.
   */
  @Override
  public UserManagedCacheBuilder<K, V, PersistentUserManagedCache<K, V>> builder(UserManagedCacheBuilder<K, V, ? extends UserManagedCache<K, V>> builder) {
    return (UserManagedCacheBuilder<K, V, PersistentUserManagedCache<K, V>>) builder.identifier(identifier).using(persistenceService);
  }
}
