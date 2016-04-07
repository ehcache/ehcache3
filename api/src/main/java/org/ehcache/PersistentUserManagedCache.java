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
 * A {@link UserManagedCache} that holds data that outlives the JVM's process
 *
 * @param <K> the type of the keys used to access data within this cache
 * @param <V> the type of the values held within this cache
 *
 * @author Alex Snaps
 */
public interface PersistentUserManagedCache<K, V> extends UserManagedCache<K, V> {

  /**
   * Destroy all persistent data structures for this {@code PersistentUserManagedCache}.
   *
   * @throws java.lang.IllegalStateException if state {@link org.ehcache.Status#MAINTENANCE} couldn't be reached
   */
  void destroy();
}
