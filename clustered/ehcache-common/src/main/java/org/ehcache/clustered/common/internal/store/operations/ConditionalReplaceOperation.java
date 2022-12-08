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

package org.ehcache.clustered.common.internal.store.operations;

import org.ehcache.clustered.common.internal.store.operations.codecs.CodecException;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.common.internal.store.operations.OperationCode.REPLACE_CONDITIONAL;

public class ConditionalReplaceOperation<K, V> implements Operation<K, V>, Result<K, V> {

  private final K key;
  private final LazyValueHolder<V> oldValueHolder;
  private final LazyValueHolder<V> newValueHolder;
  private final long timeStamp;

  public ConditionalReplaceOperation(final K key, final V oldValue, final V newValue, final long timeStamp) {
    if(key == null) {
      throw new NullPointerException("Key can not be null");
    }
    this.key = key;
    if(oldValue == null) {
      throw new NullPointerException("Old value can not be null");
    }
    this.oldValueHolder = new LazyValueHolder<>(oldValue);
    if(newValue == null) {
      throw new NullPointerException("New value can not be null");
    }
    this.newValueHolder = new LazyValueHolder<>(newValue);
    this.timeStamp = timeStamp;
  }

  ConditionalReplaceOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    if (opCode != getOpCode()) {
      throw new IllegalArgumentException("Invalid operation: " + opCode);
    }
    this.timeStamp = buffer.getLong();
    int keySize = buffer.getInt();
    int maxLimit = buffer.limit();
    buffer.limit(buffer.position() + keySize);
    ByteBuffer keyBlob = buffer.slice();
    buffer.position(buffer.limit());
    buffer.limit(maxLimit);

    int oldValueSize = buffer.getInt();
    buffer.limit(buffer.position() + oldValueSize);
    ByteBuffer oldValueBlob = buffer.slice();
    buffer.position(buffer.limit());
    buffer.limit(maxLimit);

    ByteBuffer valueBlob = buffer.slice();

    try {
      this.key = keySerializer.read(keyBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
    this.oldValueHolder = new LazyValueHolder<>(oldValueBlob, valueSerializer);
    this.newValueHolder = new LazyValueHolder<>(valueBlob, valueSerializer);
  }

  public K getKey() {
    return key;
  }

  public V getOldValue() {
    return this.oldValueHolder.getValue();
  }

  @Override
  public V getValue() {
    return this.newValueHolder.getValue();
  }

  @Override
  public PutOperation<K, V> asOperationExpiringAt(long expirationTime) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OperationCode getOpCode() {
    return REPLACE_CONDITIONAL;
  }

  @Override
  public Result<K, V> apply(Result<K, V> previousResult) {
    if(previousResult == null) {
      return null;
    } else {
      if(getOldValue().equals(previousResult.getValue())) {
        return new PutOperation<>(getKey(), getValue(), timeStamp());
      } else {
        return previousResult;
      }
    }
  }

  @Override
  public ByteBuffer encode(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    ByteBuffer keyBuf = keySerializer.serialize(key);
    ByteBuffer oldValueBuf = oldValueHolder.encode(valueSerializer);
    ByteBuffer valueBuf = newValueHolder.encode(valueSerializer);

    ByteBuffer buffer = ByteBuffer.allocate(BYTE_SIZE_BYTES +   // Operation type
                                            INT_SIZE_BYTES +    // Size of the key payload
                                            LONG_SIZE_BYTES +   // Size of expiration time stamp
                                            keyBuf.remaining() + // the key payload itself
                                            INT_SIZE_BYTES +    // Size of the old value payload
                                            oldValueBuf.remaining() +  // The old value payload itself
                                            valueBuf.remaining());  // The value payload itself

    buffer.put(getOpCode().getValue());
    buffer.putLong(this.timeStamp);
    buffer.putInt(keyBuf.remaining());
    buffer.put(keyBuf);
    buffer.putInt(oldValueBuf.remaining());
    buffer.put(oldValueBuf);
    buffer.put(valueBuf);

    buffer.flip();
    return buffer;
  }

  @Override
  public String toString() {
    return "{" + getOpCode() + "# key: " + key + ", oldValue: " + getOldValue() + ", newValue: " + getValue() + "}";
  }

  @Override
  public boolean equals(final Object obj) {
    if(obj == null) {
      return false;
    }
    if(!(obj instanceof ConditionalReplaceOperation)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    ConditionalReplaceOperation<K, V> other = (ConditionalReplaceOperation) obj;
    if(this.getOpCode() != other.getOpCode()) {
      return false;
    }
    if(!this.getKey().equals(other.getKey())) {
      return false;
    }
    if(!this.getValue().equals(other.getValue())) {
      return false;
    }
    if(!this.getOldValue().equals(other.getOldValue())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = getOpCode().hashCode();
    hash = hash * 31 + key.hashCode();
    hash = hash * 31 + getOldValue().hashCode();
    hash = hash * 31 + getValue().hashCode();
    return hash;
  }

  @Override
  public long timeStamp() {
    if (!isExpiryAvailable()) {
      return this.timeStamp;
    } else {
      throw new RuntimeException("Timestamp not available");
    }
  }

  @Override
  public boolean isExpiryAvailable() {
    return timeStamp < 0;
  }

  @Override
  public long expirationTime() {
    if (isExpiryAvailable()) {
      return - this.timeStamp;
    } else {
      throw new RuntimeException("Expiry not available");
    }
  }

}
