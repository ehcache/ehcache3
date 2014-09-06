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
 * A CacheManager that knows how to lifecycle {@link org.ehcache.Cache} data that outlive the JVM's process existence.
 *
 * @author Alex Snaps
 */
public interface PersistentCacheManager extends CacheManager {

  /**
   * Destroys all data persistent data associated with the aliased {@link Cache} instance managed
   * by this {@link org.ehcache.CacheManager}
   *
   * @param alias the {@link org.ehcache.Cache}'s alias to destroy all persistent data from
   */
  void destroyCache(String alias);

}
