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

import org.ehcache.clustered.client.internal.store.operations.Operation;
import org.ehcache.clustered.common.store.Chain;

import java.util.Map;

/**
 * Represents the result of a {@link Chain} resolution.
 * Implementors would be wrappers over the compacted chain and the resolved operations.
 * A resolver may or may not have resolved all the different keys in a chain.
 *
 * @param <K> the Key type
 */
public interface ResolvedChain<K> {

  Chain getCompactedChain();

  Operation<K> getResolvedOperation(K key);

  abstract class BaseResolvedChain<K> implements ResolvedChain<K> {

    private final Chain compactedChain;

    protected BaseResolvedChain(final Chain compactedChain) {this.compactedChain = compactedChain;}

    public Chain getCompactedChain() {
      return this.compactedChain;
    }
  }

  /**
   * Represents the {@link ResolvedChain} result of a resolver that resolves
   * all the keys in a {@link Chain}
   */
  class CombinedResolvedChain<K> extends BaseResolvedChain<K> {

    private final Map<K, Operation<K>> resolvedOperations;

    public CombinedResolvedChain(Chain compactedChain, Map<K, Operation<K>> resolvedOperations) {
      super(compactedChain);
      this.resolvedOperations = resolvedOperations;
    }

    public Operation<K> getResolvedOperation(K key) {
      return resolvedOperations.get(key);
    }
  }

  /**
   * Represents the {@link ResolvedChain} result of a resolver that resolves
   * only one key in a {@link Chain}
   */
  class SimpleResolvedChain<K> extends BaseResolvedChain<K> {

    Operation<K> resolvedOperation;

    public SimpleResolvedChain(Chain compactedChain, Operation<K> resolvedOperation) {
      super(compactedChain);
      this.resolvedOperation = resolvedOperation;
    }

    public Operation<K> getResolvedOperation(K key) {
      if(resolvedOperation != null && resolvedOperation.getKey().equals(key)) {
        return resolvedOperation;
      } else {
        return null;
      }
    }
  }
}
