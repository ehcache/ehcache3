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

import org.ehcache.ValueSupplier;
import org.ehcache.clustered.client.internal.store.ChainBuilder;
import org.ehcache.clustered.client.internal.store.ResolvedChain;
import org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.clustered.common.store.Chain;
import org.ehcache.clustered.common.store.Element;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class ChainResolver<K, V> {

  private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  private final OperationsCodec<K, V> codec;
  private final Expiry<? super K, ? super V> expiry;
  private final TimeSource timeSource;

  public ChainResolver(final OperationsCodec<K, V> codec, Expiry<? super K, ? super V> expiry, TimeSource timeSource) {
    this.codec = codec;
    this.expiry = expiry;
    this.timeSource = timeSource;
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
   */
  public ResolvedChain<K, V> resolve(Chain chain, K key, long now) {
    Result<V> result = null;
    ChainBuilder chainBuilder = new ChainBuilder();
    Operation<K, V> operation = null;
    for (Element element : chain) {
      ByteBuffer payload = element.getPayload();
      operation = codec.decode(payload);
      final Result<V> previousResult = result;
      Duration expiration;
      if(key.equals(operation.getKey())) {
        result = operation.apply(result);
        if(result == null) {
          continue;
        }
        if((previousResult == null & operation.isFirst())) {
          expiration = expiry.getExpiryForCreation(key, result.getValue());
        } else {
          expiration = expiry.getExpiryForUpdate(key, new ValueSupplier<V>() {
            @Override
            public V value() {
              return previousResult != null ? previousResult.getValue() : null;
            }
          }, result.getValue());
        }

        if(expiration == null || expiration.isInfinite()) {
          continue;
        }

        long time = TIME_UNIT.convert(expiration.getLength(), expiration.getTimeUnit());
        if(now >= time + operation.timeStamp()) {
          result = null;
        }
      } else {
        payload.flip();
        chainBuilder = chainBuilder.add(payload);
      }
    }
    Operation<K, V> resolvedOperation = null;
    if(result != null & operation != null) {
      resolvedOperation = new PutOperation<K, V>(key, result.getValue(), operation.timeStamp(), false);
      ByteBuffer payload = codec.encode(resolvedOperation);
      chainBuilder = chainBuilder.add(payload);
    }
    return new ResolvedChain.Impl<K, V>(chainBuilder.build(), key, result);
  }
}
