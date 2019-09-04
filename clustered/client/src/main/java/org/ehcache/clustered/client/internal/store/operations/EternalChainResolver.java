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

import org.ehcache.clustered.client.internal.store.ClusteredValueHolder;
import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.operations.Operation;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.Result;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.core.spi.store.Store.ValueHolder;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;

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

  @Override
  public ValueHolder<V> resolve(ServerStoreProxy.ChainEntry entry, K key, long now, int threshold) {
    PutOperation<K, V> resolved = resolve(entry, key, threshold);
    return resolved == null ? null : new ClusteredValueHolder<>(resolved.getValue());
  }

  @Override
  public Map<K, ValueHolder<V>> resolveAll(Chain chain, long now) {
    Map<K, PutOperation<K, V>> resolved = resolveAll(chain);

    Map<K, ValueHolder<V>> values = new HashMap<>(resolved.size());
    for (Map.Entry<K, PutOperation<K, V>> e : resolved.entrySet()) {
      values.put(e.getKey(), new ClusteredValueHolder<>(e.getValue().getValue()));
    }
    return unmodifiableMap(values);
  }

  /**
   * Applies the given operation returning a result that never expires.
   *
   * {@inheritDoc}
   */
  public PutOperation<K, V> applyOperation(K key, PutOperation<K, V> existing, Operation<K, V> operation) {
    final Result<K, V> newValue = operation.apply(existing);
    if (newValue == null) {
      return null;
    } else {
      return newValue.asOperationExpiringAt(Long.MAX_VALUE);
    }
  }
}
