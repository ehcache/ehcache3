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
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;

import java.nio.ByteBuffer;

public class ChainResolver<K, V> {

  private final OperationsCodec<K, V> codec;

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
   * @return an entry with the resolved operation for the provided key as the key
   * and the compacted chain as the value
   * @throws ClassNotFoundException
   */
  public ResolvedChain<K, V> resolve(Chain chain, K key) {
    Result<V> result = null;
    ChainBuilder chainBuilder = new ChainBuilder();
    for (Element element : chain) {
      ByteBuffer payload = element.getPayload();
      Operation<K, V> operation = codec.decode(payload);
      if(key.equals(operation.getKey())) {
        result = operation.apply(result);
      } else {
        payload.flip();
        chainBuilder = chainBuilder.add(payload);
      }
    }
    Operation<K, V> resolvedOperation = null;
    if(result != null) {
      resolvedOperation = new PutOperation<K, V>(key, result.getValue(), System.currentTimeMillis());
      ByteBuffer payload = codec.encode(resolvedOperation);
      chainBuilder = chainBuilder.add(payload);
    }
    return new ResolvedChain.Impl<K, V>(chainBuilder.build(), key, result);
  }
}
