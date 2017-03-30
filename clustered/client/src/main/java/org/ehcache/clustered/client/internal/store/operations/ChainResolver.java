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
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.Element;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

public class ChainResolver<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(ChainResolver.class);
  private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  private final OperationsCodec<K, V> codec;
  private final Expiry<? super K, ? super V> expiry;

  public ChainResolver(final OperationsCodec<K, V> codec, Expiry<? super K, ? super V> expiry) {
    if(expiry == null) {
      throw new NullPointerException("Expiry can not be null");
    }
    this.codec = codec;
    this.expiry = expiry;
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
    Result<V> result = null;
    ChainBuilder chainBuilder = new ChainBuilder();
    long expirationTime = Long.MIN_VALUE;
    int keyMatch = 0;
    boolean compacted = false;
    for (Element element : chain) {
      ByteBuffer payload = element.getPayload();
      Operation<K, V> operation = codec.decode(payload);
      final Result<V> previousResult = result;
      if(key.equals(operation.getKey())) {
        keyMatch++;
        result = operation.apply(result);
        if(result == null) {
          continue;
        }
        if (expiry != Expirations.noExpiration()) {
          if(operation.isExpiryAvailable()) {
            expirationTime = operation.expirationTime();
            if (expirationTime == Long.MIN_VALUE) {
              continue;
            }
            if (now >= expirationTime) {
              result = null;
            }
          } else {
            Duration duration;
            try {
              if(previousResult == null) {
                duration = expiry.getExpiryForCreation(key, result.getValue());
                if (duration == null) {
                  result = null;
                  continue;
                }
              } else {
                duration = expiry.getExpiryForUpdate(key, new ValueSupplier<V>() {
                  @Override
                  public V value() {
                    return previousResult.getValue();
                  }
                }, result.getValue());
                if (duration == null) {
                  continue;
                }
              }
            } catch (Exception ex) {
              LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", ex);
              duration = Duration.ZERO;
            }
            compacted = true;
            if(duration.isInfinite()) {
              expirationTime = Long.MIN_VALUE;
              continue;
            }
            long time = TIME_UNIT.convert(duration.getLength(), duration.getTimeUnit());
            expirationTime = time + operation.timeStamp();
            if(now >= expirationTime) {
              result = null;
            }
          }
        }
      } else {
        payload.rewind();
        chainBuilder = chainBuilder.add(payload);
      }
    }

    compacted = (result == null) ? (keyMatch > 0) : (compacted || keyMatch > 1);
    if(compacted) {
      if(result != null) {
        Operation<K, V> resolvedOperation = new PutOperation<K, V>(key, result.getValue(), -expirationTime);
        ByteBuffer payload = codec.encode(resolvedOperation);
        chainBuilder = chainBuilder.add(payload);
      }
      return new ResolvedChain.Impl<K, V>(chainBuilder.build(), key, result, keyMatch);
    } else {
      return new ResolvedChain.Impl<K, V>(chain, key, result, 0);
    }
  }
}
