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

import org.ehcache.CacheManager;

/**
 * A configuration type that enables to further specify the type of {@link CacheManager} in a
 * {@link CacheManagerBuilder}.
 *
 * @see org.ehcache.PersistentCacheManager
 */
public interface CacheManagerConfiguration<T extends CacheManager> {

  /**
   * Enables to refine the type that the {@link CacheManagerBuilder} will build.
   *
   * @param other the original builder to start from
   * @return a new builder with adapted configuration and specific {@code CacheManager} subtype
   */
  CacheManagerBuilder<T> builder(CacheManagerBuilder<? extends CacheManager> other);
}
