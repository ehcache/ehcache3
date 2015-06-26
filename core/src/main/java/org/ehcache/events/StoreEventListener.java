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

package org.ehcache.events;

import org.ehcache.spi.cache.Store;

public interface StoreEventListener<K, V> {

  /**
   * Called when an entry gets evicted during a {@code org.ehcache.spi.cache.Store} operation..
   *
   * @param key the <code>key</code> of the mapping being evicted
   * @param valueHolder the {@link Store.ValueHolder} being evicted
   */
   void onEviction(K key, Store.ValueHolder<V> valueHolder);

  /**
   * Called when an entry gets expired during a {@code org.ehcache.spi.cache.Store} operation..
   *
   * @param key the <code>key</code> of the mapping that is expired
   * @param valueHolder the {@link Store.ValueHolder} that is expired
   */
   void onExpiration(K key, Store.ValueHolder<V> valueHolder);
}
