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
 * A {@link org.ehcache.StandaloneCache} that holds data that outlives the JVM's process
 *
 * @author Alex Snaps
 */
public interface PersistentStandaloneCache<K, V> extends StandaloneCache<K, V> {

  /**
   * Lets you manipulate the persistent data structures for this {@link org.ehcache.PersistentStandaloneCache}
   *
   * @return a {@link org.ehcache.Maintainable} for this {@link org.ehcache.PersistentStandaloneCache}
   * @throws java.lang.IllegalStateException if state {@link org.ehcache.Status#MAINTENANCE} couldn't be reached
   */
  Maintainable toMaintenance();
}
