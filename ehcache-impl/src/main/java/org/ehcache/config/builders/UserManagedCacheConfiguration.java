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

package org.ehcache.config.builders;

import org.ehcache.UserManagedCache;

/**
 * A configuration type that enables to further specify the type of {@link UserManagedCache} in a
 * {@link UserManagedCacheBuilder}.
 *
 * @see org.ehcache.PersistentUserManagedCache
 */
public interface UserManagedCacheConfiguration<K, V, T extends UserManagedCache<K, V>> {

  /**
   * Enables to refine the type that the {@link UserManagedCacheBuilder} will build.
   *
   * @param builder the original builder to start from
   * @return a new builder with adapted configuration and specific {@code UserManagedCache} subtype
   */
  UserManagedCacheBuilder<K, V, T> builder(UserManagedCacheBuilder<K, V, ? extends UserManagedCache<K, V>> builder);
}
