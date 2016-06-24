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

import org.ehcache.spi.serialization.Serializer;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * {@link SoftLock}s are the value containers stored in the underlying store by the {@link XAStore}.
 * A {@link SoftLock} contains three essential things:
 * <ul>
 * <li>The transaction ID responsible for inserting the {@link SoftLock}, or null if the transaction is terminated.</li>
 * <li>The old value, i.e.: the value that is going to be stored upon transaction rollback, or null if there was no value.</li>
 * <li>The new value holder, i.e.: the value holder that is going to be stored upon transaction commit, or null if the mapping is to be removed.</li>
 * </ul>
 *
 * @author Ludovic Orban
 */
public class SoftLock<V> implements Serializable {

  private final TransactionId transactionId;
  private final V oldValue;
  private final byte[] oldValueSerialized;
  private final XAValueHolder<V> newValueHolder;

  public SoftLock(TransactionId transactionId, V oldValue, XAValueHolder<V> newValueHolder) {
    this.transactionId = transactionId;
    this.oldValue = oldValue;
    this.oldValueSerialized = null;
    this.newValueHolder = newValueHolder;
  }

  private SoftLock(TransactionId transactionId, ByteBuffer serializedOldValue, XAValueHolder<V> serializedNewValueHolder) {
    this.transactionId = transactionId;
    this.oldValue = null;
    if (serializedOldValue != null) {
      this.oldValueSerialized = new byte[serializedOldValue.remaining()];
      serializedOldValue.get(oldValueSerialized);
    } else {
      oldValueSerialized = null;
    }
    this.newValueHolder = serializedNewValueHolder;
  }

  protected SoftLock<V> copyForSerialization(Serializer<V> valueSerializer) {
    ByteBuffer serializedOldValue = null;
    if (oldValue != null) {
      serializedOldValue = valueSerializer.serialize(oldValue);
    }
    XAValueHolder<V> serializedXaValueHolder = null;
    if (newValueHolder != null) {
      serializedXaValueHolder = newValueHolder.copyForSerialization(valueSerializer);
    }
    return new SoftLock<V>(transactionId, serializedOldValue, serializedXaValueHolder);
  }

  protected SoftLock<V> copyAfterDeserialization(Serializer<V> valueSerializer, SoftLock<V> serializedSoftLock) throws ClassNotFoundException {
    V oldValue = null;
    if (serializedSoftLock.oldValueSerialized != null) {
      oldValue = valueSerializer.read(ByteBuffer.wrap(serializedSoftLock.oldValueSerialized));
    }
    XAValueHolder<V> newValueHolder = null;
    if (this.newValueHolder != null) {
      newValueHolder = this.newValueHolder.copyAfterDeserialization(valueSerializer);
    }
    return new SoftLock<V>(transactionId, oldValue, newValueHolder);
  }

  public V getOldValue() {
    return oldValue;
  }

  public XAValueHolder<V> getNewValueHolder() {
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

    if (transactionId != null ? !transactionId.equals(softLock.transactionId) : softLock.transactionId != null)
      return false;
    if (oldValue != null ? !oldValue.equals(softLock.oldValue) : softLock.oldValue != null)
      return false;
    return !(newValueHolder != null ? !newValueHolder.equals(softLock.newValueHolder) : softLock.newValueHolder != null);

  }

  @Override
  public int hashCode() {
    int result = transactionId != null ? transactionId.hashCode() : 0;
    result = 31 * result + (oldValue != null ? oldValue.hashCode() : 0);
    result = 31 * result + (newValueHolder != null ? newValueHolder.hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "SoftLock TxId[" + transactionId + "] Old[" + oldValue + "] New[" + newValueHolder + "]";
  }
}
