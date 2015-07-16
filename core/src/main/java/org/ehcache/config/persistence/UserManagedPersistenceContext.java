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

package org.ehcache.config.persistence;

import org.ehcache.PersistentUserManagedCache;
import org.ehcache.UserManagedCache;
import org.ehcache.UserManagedCacheBuilder;
import org.ehcache.config.UserManagedCacheConfiguration;
import org.ehcache.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.ServiceConfiguration;

import java.io.File;

/**
 * UserManagedPersistenceContext
 */
public class UserManagedPersistenceContext<K, V> implements UserManagedCacheConfiguration<K, V, PersistentUserManagedCache<K, V>> {

  private final String identifier;
  private final LocalPersistenceService persistenceService;

  public UserManagedPersistenceContext(String identifier, LocalPersistenceService persistenceService) {
    this.identifier = identifier;
    this.persistenceService = persistenceService;
  }

  @Override
  public UserManagedCacheBuilder<K, V, PersistentUserManagedCache<K, V>> builder(UserManagedCacheBuilder<K, V, ? extends UserManagedCache<K, V>> builder) {
    return (UserManagedCacheBuilder<K, V, PersistentUserManagedCache<K, V>>) builder.identifier(identifier).using(persistenceService);
  }
}
