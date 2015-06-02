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

import org.ehcache.expiry.Expiry;

/**
 * @author Alex Snaps
 */
public interface TerracottaCacheConfiguration<K, V> extends CacheConfiguration<K, V> {

  ClusteredCacheSharedConfiguration<K, V> getClusteredCacheConfiguration();

  /**
   * {@inheritDoc}
   *
   * Delegates to {@link ClusteredCacheSharedConfiguration#getKeyType()}
   */
  Class<K> getKeyType();

  /**
   * {@inheritDoc}
   *
   * Delegates to {@link ClusteredCacheSharedConfiguration#getValueType()}
   */
  Class<V> getValueType();

  /**
   * {@inheritDoc}
   *
   * Delegates to {@link ClusteredCacheSharedConfiguration#getEvictionVeto()}
   */
  EvictionVeto<? super K, ? super V> getEvictionVeto();

  /**
   * {@inheritDoc}
   *
   * Delegates to {@link ClusteredCacheSharedConfiguration#getEvictionPrioritizer()}
   */
  EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer();

  /**
   * {@inheritDoc}
   *
   * Delegates to {@link ClusteredCacheSharedConfiguration#getExpiry()}
   */
  Expiry<? super K, ? super V> getExpiry();

}
