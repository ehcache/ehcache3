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

import org.ehcache.config.CacheConfiguration;

/**
 * A CacheManager that manages {@link Cache} as well as associated {@link org.ehcache.spi.service.Service}
 *
 * @author Alex Snaps
 */
public interface CacheManager {

  <K, V> Cache<K, V> createCache(String alias, CacheConfiguration<K, V> config);

  <K, V> Cache<K, V> getCache(String alias, Class<K> keyType, Class<V> valueType);

  void removeCache(String alias);

  /**
   * Attempts at having this CacheManager go to {@link org.ehcache.Status#AVAILABLE}, starting all
   * {@link org.ehcache.spi.service.Service} instances managed by this {@link org.ehcache.CacheManager}, as well
   * as all {@link org.ehcache.Cache} pre registered with it.
   *
   * @throws java.lang.IllegalStateException if the CacheManager isn't in {@link org.ehcache.Status#UNINITIALIZED} state
   */
  void init();

  /**
   * Releases all data held in {@link Cache} instances managed by this {@link CacheManager}, as well as all
   * {@link org.ehcache.spi.service.Service} this instance provides to managed {@link Cache} instances.
   */
  void close();

  Status getStatus();
}
