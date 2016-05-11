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

import org.ehcache.clustered.common.ServerStoreConfiguration;
import org.ehcache.clustered.common.store.ServerStore;

/**
 * Provides client-side access to the services of a {@code ServerStore}.
 */
public interface ServerStoreProxy<K, V> extends ServerStore {

  /**
   * Gets the identifier linking a client-side cache to a {@code ServerStore} instance.
   *
   * @return the cache identifier
   */
  String getCacheId();

  /**
   * Gets the cache-exposed key type used in the {@code ServerStore}.
   *
   * @return the key type
   */
  Class<K> getKeyType();

  /**
   * Gets the cache-exposed value type used in the {@code ServerStore}.
   *
   * @return the value type
   */
  Class<V> getValueType();

  /**
   * Gets the {@link ServerStoreConfiguration} used to create this {@code ServerStoreProxy}.
   *
   * @return the {@code ServerStoreConfiguration}
   */
  ServerStoreConfiguration getServerStoreConfiguration();
}
