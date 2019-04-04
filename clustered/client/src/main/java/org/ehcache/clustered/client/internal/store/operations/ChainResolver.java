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

import org.ehcache.clustered.client.internal.store.ServerStoreProxy;
import org.ehcache.clustered.common.internal.util.ChainBuilder;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.clustered.common.internal.store.operations.Operation;
import org.ehcache.clustered.common.internal.store.operations.PutOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.core.spi.store.Store.ValueHolder;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

/**
 * An abstract chain resolver.
 * <p>
 * Operation application is performed in subclasses specialized for eternal and non-eternal caches.
 *
 * @see EternalChainResolver
 * @see ExpiryChainResolver
 *
 * @param <K> key type
 * @param <V> value type
 */
public abstract class ChainResolver<K, V> {
  protected final OperationsCodec<K, V> codec;

  public ChainResolver(final OperationsCodec<K, V> codec) {
    this.codec = codec;
  }

  /**
   * Resolves the given key within the given chain entry to its current value with a specific compaction threshold.
   * <p>
   * If the resultant chain has shrunk by more than {@code threshold} elements then an attempt is made to perform the
   * equivalent compaction on the server.
   *
   * @param entry target chain entry
   * @param key target key
   * @param now current time
   * @param threshold compaction threshold
   * @return the current value
   */
  public abstract ValueHolder<V> resolve(ServerStoreProxy.ChainEntry entry, K key, long now, int threshold);

  /**
   * Resolves the given key within the given chain entry to its current value.
   * <p>
   * This is exactly equivalent to calling {@link #resolve(ServerStoreProxy.ChainEntry, Object, long, int)} with a zero
   * compaction threshold.
   *
   * @param entry target chain entry
   * @param key target key
   * @param now current time
   * @return the current value
   */
  public ValueHolder<V> resolve(ServerStoreProxy.ChainEntry entry, K key, long now) {
    return resolve(entry, key, now, 0);
  }

  /**
   * Resolves all keys within the given chain to their current values.
   *
   * @param chain target chain
   * @param now current time
   * @return a map of current values
   */
  public abstract Map<K, ValueHolder<V>> resolveAll(Chain chain, long now);

  /**
   * Compacts the given chain entry by resolving every key within.
   *
   * @param entry an uncompacted heterogenous {@link ServerStoreProxy.ChainEntry}
   */
  public void compact(ServerStoreProxy.ChainEntry entry) {
    ChainBuilder builder = new ChainBuilder();
    for (PutOperation<K, V> operation : resolveAll(entry).values()) {
      builder = builder.add(codec.encode(operation));
    }
    Chain compacted = builder.build();
    if (compacted.length() < entry.length()) {
      entry.replaceAtHead(compacted);
    }
  }

  /**
   * Resolves the given key within the given chain entry to an equivalent put operation.
   * <p>
   * If the resultant chain has shrunk by more than {@code threshold} elements then an attempt is made to perform the
   * equivalent compaction on the server.
   *
   * @param entry target chain entry
   * @param key target key
   * @param threshold compaction threshold
   * @return equivalent put operation
   */
  protected PutOperation<K, V> resolve(ServerStoreProxy.ChainEntry entry, K key, int threshold) {
    PutOperation<K, V> result = null;
    ChainBuilder resolvedChain = new ChainBuilder();
    for (Element element : entry) {
      ByteBuffer payload = element.getPayload();
      Operation<K, V> operation = codec.decode(payload);

      if(key.equals(operation.getKey())) {
        result = applyOperation(key, result, operation);
      } else {
        payload.rewind();
        resolvedChain = resolvedChain.add(payload);
      }
    }
    if(result != null) {
      resolvedChain = resolvedChain.add(codec.encode(result));
    }

    if (entry.length() - resolvedChain.length() > threshold) {
      entry.replaceAtHead(resolvedChain.build());
    }
    return result;
  }

  /**
   * Resolves all keys within the given chain to their equivalent put operations.
   *
   * @param chain target chain
   * @return a map of equivalent put operations
   */
  protected Map<K, PutOperation<K, V>> resolveAll(Chain chain) {
    //absent hash-collisions this should always be a 1 entry map
    Map<K, PutOperation<K, V>> compacted = new HashMap<>(2);
    for (Element element : chain) {
      ByteBuffer payload = element.getPayload();
      Operation<K, V> operation = codec.decode(payload);
      compacted.compute(operation.getKey(), (k, v) -> applyOperation(k, v, operation));
    }
    return compacted;
  }

  /**
   * Applies the given operation to the current state.
   *
   * @param key cache key
   * @param existing current state
   * @param operation operation to apply
   * @return an equivalent put operation
   */
  public abstract PutOperation<K, V> applyOperation(K key, PutOperation<K, V> existing, Operation<K, V> operation);
}
