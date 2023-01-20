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

package org.ehcache.transactions.xa.internal;

import org.ehcache.impl.internal.concurrent.ConcurrentHashMap;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.core.spi.store.Store;
import org.ehcache.transactions.xa.internal.journal.Journal;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
public class XATransactionContextFactory<K, V> {

  private final Map<TransactionId, XATransactionContext<K, V>> transactionContextMap = new ConcurrentHashMap<TransactionId, XATransactionContext<K, V>>();
  private final TimeSource timeSource;

  public XATransactionContextFactory(TimeSource timeSource) {
    this.timeSource = timeSource;
  }

  public XATransactionContext<K, V> createTransactionContext(TransactionId transactionId, Store<K, SoftLock<V>> underlyingStore, Journal<K> journal, int transactionTimeoutInSeconds) {
    long nowTimestamp = timeSource.getTimeMillis();
    long timeoutTimestamp = nowTimestamp + TimeUnit.SECONDS.toMillis(transactionTimeoutInSeconds);
    XATransactionContext<K, V> transactionContext = new XATransactionContext<K, V>(transactionId, underlyingStore, journal, timeSource, timeoutTimestamp);
    transactionContextMap.put(transactionId, transactionContext);
    return transactionContext;
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
    return transactionContext.newValueHolders();
  }

  public boolean isTouched(TransactionId transactionId, K key) {
    XATransactionContext<K, V> transactionContext = transactionContextMap.get(transactionId);
    if (transactionContext == null) {
      throw new IllegalStateException("Cannot check for removed key outside of transactional context");
    }
    return transactionContext.touched(key);
  }
}
