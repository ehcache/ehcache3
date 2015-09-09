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
import org.ehcache.spi.cache.Store;
import org.ehcache.transactions.journal.Journal;

import java.util.Map;

/**
 * @author Ludovic Orban
 */
public class XATransactionContextFactory<K, V> {

  private final Map<TransactionId, XATransactionContext<K, V>> transactionContextMap = new ConcurrentHashMap<TransactionId, XATransactionContext<K, V>>();

  public void create(TransactionId transactionId, Store<K, SoftLock<V>> underlyingStore, Journal journal) {
    XATransactionContext<K, V> transactionContext = new XATransactionContext<K, V>(transactionId, underlyingStore, journal);
    transactionContextMap.put(transactionId, transactionContext);
  }

  public XATransactionContext<K, V> get(TransactionId transactionId) {
    return transactionContextMap.get(transactionId);
  }

  public void destroy(TransactionId transactionId) {
    transactionContextMap.remove(transactionId);
  }

  public boolean contains(TransactionId transactionId) {
    return transactionContextMap.containsKey(transactionId);
  }

  public Map<K, XAValueHolder<V>> listPuts(TransactionId transactionId) {
    XATransactionContext<K, V> transactionContext = transactionContextMap.get(transactionId);
    if (transactionContext == null) {
      throw new IllegalStateException("Cannot check for removed key outside of transactional context");
    }
    return transactionContext.listPutNewValueHolders();
  }

  public boolean isTouched(TransactionId transactionId, K key) {
    XATransactionContext<K, V> transactionContext = transactionContextMap.get(transactionId);
    if (transactionContext == null) {
      throw new IllegalStateException("Cannot check for removed key outside of transactional context");
    }
    return transactionContext.touched(key);
  }
}
