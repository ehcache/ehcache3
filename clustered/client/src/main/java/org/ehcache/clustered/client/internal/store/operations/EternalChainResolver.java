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

package org.ehcache.clustered.client.internal.store.operations;

import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;

/**
 * A specialized chain resolver for eternal caches.
 *
 * @see org.ehcache.expiry.Expirations#noExpiration()
 *
 * @param <K> key type
 * @param <V> value type
 */
public class EternalChainResolver<K, V> extends ChainResolver<K, V> {

  public EternalChainResolver(final OperationsCodec<K, V> codec) {
    super(codec);
  }

  /**
   * Applies the given operation returning a result that never expires.
   *
   * @param key cache key
   * @param existing current state
   * @param operation operation to apply
   * @param now current time
   * @return the equivalent put operation
   */
  protected PutOperation<K, V> applyOperation(K key, PutOperation<K, V> existing, Operation<K, V> operation, long now) {
    final Result<K, V> newValue = operation.apply(existing);
    if (newValue == null) {
      return null;
    } else {
      return newValue.asOperationExpiringAt(Long.MAX_VALUE);
    }
  }
}
