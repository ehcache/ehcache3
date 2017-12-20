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

import org.ehcache.clustered.client.internal.store.ChainBuilder;
import org.ehcache.clustered.client.internal.store.ResolvedChain;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  protected static final Logger LOG = LoggerFactory.getLogger(EternalChainResolver.class);
  protected final OperationsCodec<K, V> codec;

  public ChainResolver(final OperationsCodec<K, V> codec) {
    this.codec = codec;
  }

  /**
   * Extract the {@code Element}s from the provided {@code Chain} that are not associated with the provided key
   * and create a new {@code Chain}
   *
   * Separate the {@code Element}s from the provided {@code Chain} that are associated and not associated with
   * the provided key. Create a new chain with the unassociated {@code Element}s. Resolve the associated elements
   * and append the resolved {@code Element} to the newly created chain.
   *
   * @param chain a heterogeneous {@code Chain}
   * @param key a key
   * @param now time when the chain is being resolved
   * @return a resolved chain, result of resolution of chain provided
   */
  public ResolvedChain<K, V> resolve(Chain chain, K key, long now) {
    PutOperation<K, V> result = null;
    ChainBuilder newChainBuilder = new ChainBuilder();
    boolean matched = false;
    for (Element element : chain) {
      ByteBuffer payload = element.getPayload();
      Operation<K, V> operation = codec.decode(payload);

      if(key.equals(operation.getKey())) {
        matched = true;
        result = applyOperation(key, result, operation, now);
      } else {
        payload.rewind();
        newChainBuilder = newChainBuilder.add(payload);
      }
    }

    if(result == null) {
      if (matched) {
        Chain newChain = newChainBuilder.build();
        return new ResolvedChain.Impl<>(newChain, key, null, chain.length() - newChain.length(), Long.MAX_VALUE);
      } else {
        return new ResolvedChain.Impl<>(chain, key, null, 0, Long.MAX_VALUE);
      }
    } else {
      Chain newChain = newChainBuilder.add(codec.encode(result)).build();
      return new ResolvedChain.Impl<>(newChain, key, result, chain.length() - newChain.length(), result.expirationTime());
    }
  }

  /**
   * Compacts the given chain by resolving every key within.
   *
   * @param chain a compacted heterogenous {@code Chain}
   * @param now time when the chain is being resolved
   * @return a compacted chain
   */
  public Chain applyOperation(Chain chain, long now) {
    //absent hash-collisions this should always be a 1 entry map
    Map<K, PutOperation<K, V>> compacted = new HashMap<>(2);
    for (Element element : chain) {
      ByteBuffer payload = element.getPayload();
      Operation<K, V> operation = codec.decode(payload);
      compacted.compute(operation.getKey(), (k, v) -> applyOperation(k, v, operation, now));
    }

    ChainBuilder builder = new ChainBuilder();
    for (PutOperation<K, V> operation : compacted.values()) {
      builder = builder.add(codec.encode(operation));
    }
    return builder.build();
  }

  /**
   * Applies the given operation to the current state at the time specified.
   *
   * @param key cache key
   * @param existing current state
   * @param operation operation to apply
   * @param now current time
   * @return an equivalent put operation
   */
  protected abstract PutOperation<K, V> applyOperation(K key, PutOperation<K, V> existing, Operation<K, V> operation, long now);
}
