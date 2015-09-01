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

import org.ehcache.spi.cache.Store;

import java.io.Serializable;

/**
 * @author Ludovic Orban
 */
public class SoftLock<V> implements Serializable {

  private final TransactionId transactionId;
  private final Store.ValueHolder<V> oldValueHolder;
  private final Store.ValueHolder<V> newValueHolder;

  public SoftLock(TransactionId transactionId, Store.ValueHolder<V> oldValueHolder, Store.ValueHolder<V> newValueHolder) {
    this.transactionId = transactionId;
    this.oldValueHolder = oldValueHolder;
    this.newValueHolder = newValueHolder;
  }

  public Store.ValueHolder<V> getOldValueHolder() {
    return oldValueHolder;
  }
  public Store.ValueHolder<V> getNewValueHolder() {
    return newValueHolder;
  }

  public TransactionId getTransactionId() {
    return transactionId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    SoftLock<?> softLock = (SoftLock<?>) o;

    if (!transactionId.equals(softLock.transactionId)) return false;
    if (oldValueHolder != null ? !oldValueHolder.equals(softLock.oldValueHolder) : softLock.oldValueHolder != null)
      return false;
    return !(newValueHolder != null ? !newValueHolder.equals(softLock.newValueHolder) : softLock.newValueHolder != null);

  }

  @Override
  public int hashCode() {
    int result = transactionId.hashCode();
    result = 31 * result + (oldValueHolder != null ? oldValueHolder.hashCode() : 0);
    result = 31 * result + (newValueHolder != null ? newValueHolder.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "SoftLock TxId[" + transactionId + "] Old[" + oldValueHolder + "] New[" + newValueHolder + "]";
  }
}
