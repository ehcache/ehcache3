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
package org.ehcache.transactions;

import org.ehcache.internal.concurrent.ConcurrentHashMap;

/**
 * @author Ludovic Orban
 */
public class TransientXaTransactionStateStore implements XaTransactionStateStore {
  private final ConcurrentHashMap<TransactionId, XAState> states = new ConcurrentHashMap<TransactionId, XAState>();

  @Override
  public void save(TransactionId transactionId, XAState xaState) {
    if (xaState == XAState.IN_DOUBT) {
      states.put(transactionId, xaState);
    } else {
      states.remove(transactionId);
    }
  }

  @Override
  public XAState getState(TransactionId transactionId) {
    return states.get(transactionId);
  }
}
