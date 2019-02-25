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

package org.ehcache.config;

import org.ehcache.config.CacheConfiguration;

/**
 * A fluent builder of {@link CacheConfiguration} instances.
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
public interface FluentCacheConfigurationBuilder<K, V> extends Builder<CacheConfiguration<K, V>> {

  /**
   * Builds a new {@link CacheConfiguration}.
   *
   * @return a new {@code CacheConfiguration}
   */
  CacheConfiguration<K, V> build();
}

