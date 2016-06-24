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

package org.ehcache;

/**
 * A {@link UserManagedCache} that holds data that can outlive the JVM.
 *
 * @param <K> the key type for the cache
 * @param <V> the value type for the cache
 */
public interface PersistentUserManagedCache<K, V> extends UserManagedCache<K, V> {

  /**
   * Destroys all persistent data structures for this {@code PersistentUserManagedCache}.
   *
   * @throws java.lang.IllegalStateException if state {@link org.ehcache.Status#MAINTENANCE MAINTENANCE} couldn't be reached
   * @throws CachePersistenceException if the persistent data cannot be destroyed
   */
  void destroy() throws CachePersistenceException;
}
