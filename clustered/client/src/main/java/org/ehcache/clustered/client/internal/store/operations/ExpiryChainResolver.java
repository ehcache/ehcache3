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
import org.ehcache.clustered.common.internal.store.operations.TimestampOperation;
import org.ehcache.clustered.common.internal.store.operations.codecs.OperationsCodec;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.core.spi.store.Store.ValueHolder;
import org.ehcache.expiry.ExpiryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;
import static org.ehcache.core.config.ExpiryUtils.isExpiryDurationInfinite;

/**
 * A specialized chain resolver for non-eternal caches.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ExpiryChainResolver<K, V> extends ChainResolver<K, V> {

  private static final Logger LOG = LoggerFactory.getLogger(ExpiryChainResolver.class);

  private final ExpiryPolicy<? super K, ? super V> expiry;

  /**
   * Creates a resolver with the given codec and expiry policy.
   *
   * @param codec operation codec
   * @param expiry expiry policy
   */
  public ExpiryChainResolver(final OperationsCodec<K, V> codec, ExpiryPolicy<? super K, ? super V> expiry) {
    super(codec);
    this.expiry = requireNonNull(expiry, "Expiry cannot be null");
  }

  @Override
  public ValueHolder<V> resolve(ServerStoreProxy.ChainEntry entry, K key, long now, int threshold) {
    PutOperation<K, V> resolved = resolve(entry, key, threshold);

    if (resolved == null) {
      return null;
    } else if (now >= resolved.expirationTime()) {
      try {
        entry.append(codec.encode(new TimestampOperation<>(key, now)));
      } catch (TimeoutException e) {
        LOG.debug("Failed to append timestamp operation", e);
      }
      return null;
    } else {
      return new ClusteredValueHolder<>(resolved.getValue(), resolved.expirationTime());
    }
  }

  @Override
  public Map<K, ValueHolder<V>> resolveAll(Chain chain, long now) {
    Map<K, PutOperation<K, V>> resolved = resolveAll(chain);

    Map<K, ValueHolder<V>> values = new HashMap<>(resolved.size());
    for (Map.Entry<K, PutOperation<K, V>> e : resolved.entrySet()) {
      if (now < e.getValue().expirationTime()) {
        values.put(e.getKey(), new ClusteredValueHolder<>(e.getValue().getValue(), e.getValue().expirationTime()));
      }
    }
    return unmodifiableMap(values);
  }

  /**
   * Applies the given operation returning a result with an expiry time determined by this resolvers expiry policy.
   * <p>
   * If the resolved operations expiry time has passed then {@code null} is returned.
   *
   * @param key cache key
   * @param existing current state
   * @param operation operation to apply
   * @param now current time
   * @return the equivalent put operation
   */
  @Override
  public PutOperation<K, V> applyOperation(K key, PutOperation<K, V> existing, Operation<K, V> operation) {
    if (existing != null && operation.timeStamp() >= existing.expirationTime()) {
      existing = null;
    }

    final Result<K, V> newValue = operation.apply(existing);
    if (newValue == null) {
      return null;
    } else if (newValue == existing) {
      return existing;
    } else {
      return newValue.asOperationExpiringAt(calculateExpiryTime(key, existing, operation, newValue));
    }
  }

  /**
   * Calculates the expiration time of the new state based on this resolvers expiry policy.
   *
   * @param key cache key
   * @param existing current state
   * @param operation operation to apply
   * @param newValue new state
   * @return the calculated expiry time
   */
  private long calculateExpiryTime(K key, PutOperation<K, V> existing, Operation<K, V> operation, Result<K, V> newValue) {
    if (operation.isExpiryAvailable()) {
      return operation.expirationTime();
    } else {
      try {
        Duration duration;
        if (existing == null) {
          duration = requireNonNull(expiry.getExpiryForCreation(key, newValue.getValue()));
        } else {
          duration = expiry.getExpiryForUpdate(key, existing::getValue, newValue.getValue());
          if (duration == null) {
            return existing.expirationTime();
          }
        }
        if (duration.isNegative()) {
          duration = Duration.ZERO;
        } else if (isExpiryDurationInfinite(duration)) {
          return Long.MAX_VALUE;
        }
        return ExpiryUtils.getExpirationMillis(operation.timeStamp(), duration);
      } catch (Exception ex) {
        LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", ex);
        return Long.MIN_VALUE;
      }
    }
  }
}
