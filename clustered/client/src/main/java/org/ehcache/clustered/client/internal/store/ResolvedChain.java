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

import org.ehcache.clustered.client.internal.store.operations.Result;
import org.ehcache.clustered.common.internal.store.Chain;

import java.util.Collections;
import java.util.Map;

/**
 * Represents the result of a {@link Chain} resolution.
 * Implementors would be wrappers over the compacted chain and the resolved operations.
 * A resolver may or may not have resolved all the different keys in a chain.
 *
 * @param <K> the Key type
 */
public interface ResolvedChain<K, V> {

  Chain getCompactedChain();

  Result<K, V> getResolvedResult(K key);

  /**
   * Indicates whether the {@link #getCompactedChain()} is effectively compacted
   * compared to the original chain it was built from.
   *
   * @return {@code true} if the chain has been compacted during resolution, {@code false} otherwise
   */
  boolean isCompacted();

  /**
   * @return the number of chain elements that were compacted if there was any compaction
   */
  int getCompactionCount();

  /**
   * @return the unix epoch at which the entry should expire
   */
  long getExpirationTime();

  /**
   * Represents the {@link ResolvedChain} result of a resolver that resolves
   * all the keys in a {@link Chain}
   */
  class Impl<K, V> implements ResolvedChain<K, V> {

    private final Chain compactedChain;
    private final Map<K, Result<K, V>> resolvedOperations;
    private final int compactionCount;
    private final long expirationTime;

    public Impl(Chain compactedChain, Map<K, Result<K, V>> resolvedOperations, int compactionCount, long expirationTime) {
      this.compactedChain = compactedChain;
      this.resolvedOperations = resolvedOperations;
      this.compactionCount = compactionCount;
      this.expirationTime = expirationTime;
    }

    public Impl(Chain compactedChain, K key, Result<K, V> result, int compactedSize, long expirationTime) {
      this(compactedChain, Collections.singletonMap(key, result), compactedSize, expirationTime);
    }

    public Chain getCompactedChain() {
      return this.compactedChain;
    }

    public Result<K, V> getResolvedResult(K key) {
      return resolvedOperations.get(key);
    }

    @Override
    public boolean isCompacted() {
      return compactionCount > 0;
    }

    public int getCompactionCount() {
      return compactionCount;
    }

    @Override
    public long getExpirationTime() {
      return expirationTime;
    }
  }
}
