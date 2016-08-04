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
package org.ehcache.clustered.client.internal.store;

import org.ehcache.clustered.common.internal.store.ServerStore;

/**
 * @author Ludovic Orban
 */
public interface ServerStoreProxy extends ServerStore {

  /**
   * The invalidation listener
   */
  interface InvalidationListener {
    /**
     * Callback for invalidation of hash requests
     *
     * @param hash the hash of the keys to invalidate
     */
    void onInvalidateHash(long hash);

    /**
     * Callback for invalidation of all requests
     */
    void onInvalidateAll();
  }

  /**
   * Gets the identifier linking a client-side cache to a {@code ServerStore} instance.
   *
   * @return the cache identifier
   */
  String getCacheId();

  /**
   * Add a listener called when invalidation requests arrive.
   *
   * @param listener the listener to add
   */
  void addInvalidationListener(InvalidationListener listener);

  /**
   * Remove a listener
   *
   * @param listener the listener to remove
   */
  boolean removeInvalidationListener(InvalidationListener listener);

}
