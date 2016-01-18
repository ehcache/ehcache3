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

  /**
   * Called when an entry gets created during a {@code org.ehcache.spi.cache.Store} operation.
   *
   * @param key the <code>key</code> of the mapping being created
   * @param valueHolder the {@link org.ehcache.spi.cache.Store.ValueHolder} being created
   */
  void onCreation(K key, Store.ValueHolder<V> valueHolder);

  /**
   * Called when an entry gets updated during a {@code Store} operation.
   *
   * @param key the <code>key</code> of the mapping being updated
   * @param previousValue the previous {@link org.ehcache.spi.cache.Store.ValueHolder}
   * @param newValue the new {@link org.ehcache.spi.cache.Store.ValueHolder}
   */
  void onUpdate(K key, Store.ValueHolder<V> previousValue, Store.ValueHolder<V> newValue);

  /**
   * Called when an entry gets removed during a {@code Store} operation.
   *
   * @param key the <code>key</code> of the mapping being removed
   * @param removed the {@link org.ehcache.spi.cache.Store.ValueHolder} being removed
   */
  void onRemoval(K key, Store.ValueHolder<V> removed);

  /**
   * Indicates if there are listeners currently registered.
   *
   * @return {@code true} if there are listeners, {@code false} otherwise
   */
  boolean hasListeners();

  /**
   * Triggers the firing of events that have been queued.
   */
  void fireAllEvents();

  /**
   * Invoked as a finally, will either be a no-op or will clean up queued events, firing some
   *
   * TODO Contract sounds weird - revisit
   */
  void purgeOrFireRemainingEvents();
}
