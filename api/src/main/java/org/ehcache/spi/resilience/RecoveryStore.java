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

package org.ehcache.spi.resilience;

import org.ehcache.spi.resilience.StoreAccessException;

/**
 * A recovery store is used during entry cleanup done by the {@link ResilienceStrategy}. It's called
 * when a {@link org.ehcache.core.spi.store.Store} failed on an entry. Implementations will in general want to get rid
 * of this entry which is what the recovery store is used for.
 * <p>
 * Note that the methods on this call with tend to fail since the store already failed once and caused the resilience
 * strategy to be called.
 *
 * @param <K> store key type
 */
public interface RecoveryStore<K> {

  /**
   * Obliterate all keys in a store.
   *
   * @throws StoreAccessException in case of store failure
   */
  void obliterate() throws StoreAccessException;

  /**
   * Obliterate a given key.
   *
   * @param key the key to obliterate
   * @throws StoreAccessException in case of store failure
   */
  void obliterate(K key) throws StoreAccessException;

  /**
   * Obliterate a list of keys.
   *
   * @param keys keys to obliterate
   * @throws StoreAccessException in case of store failure
   */
  default void obliterate(Iterable<? extends K> keys) throws StoreAccessException {
    for (K key : keys) {
      obliterate(key);
    }
  }

}
