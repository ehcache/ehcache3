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

package org.ehcache.clustered;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.Maintainable;
import org.ehcache.clustered.exceptions.CacheDurabilityException;

/**
 * Specifies a {@link CacheManager} supporting clustered operations.
 *
 * @author Clifford W. Johnson
 */
public interface ClusteredCacheManager extends CacheManager {

  /**
   * Lets you manipulate the durable, clustered data structures for this {@code ClusteredCacheManager}.
   *
   * @return a {@link org.ehcache.Maintainable} for this {@link ClusteredCacheManager}
   * @throws java.lang.IllegalStateException if state {@link org.ehcache.Status#MAINTENANCE} couldn't be reached
   */
  Maintainable toMaintenance();

  /**
   * Destroys all durable data associated with the aliased {@link Cache} instance managed
   * by this {@code ClusteredCacheManager}.
   *
   * @param alias the alias of the {@link Cache} in which all durable, clustered data is destroyed
   *
   * @throws CacheDurabilityException When something goes wrong destroying the durable data
   */
  void destroyCache(String alias) throws CacheDurabilityException;

}
