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

import org.ehcache.clustered.client.internal.store.operations.codecs.CodecException;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.client.internal.store.operations.OperationCode.REPLACE_CONDITIONAL;

public class ConditionalReplaceOperation<K, V> implements Operation<K, V>, Result<V> {

  private final K key;
  private final V oldValue;
  private final V newValue;
  private final long timeStamp;
  private final byte isFirst;


  public ConditionalReplaceOperation(final K key, final V oldValue, final V newValue, final long timeStamp, final boolean isFirst) {
    if(key == null) {
      throw new NullPointerException("Key can not be null");
    }
    this.key = key;
    if(oldValue == null) {
      throw new NullPointerException("Old value can not be null");
    }
    this.oldValue = oldValue;
    if(newValue == null) {
      throw new NullPointerException("New value can not be null");
    }
    this.newValue = newValue;
    this.timeStamp = timeStamp;
    if (isFirst) {
      this.isFirst = (byte)1;
    } else {
      this.isFirst = (byte)0;
    }
  }

  ConditionalReplaceOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    if (opCode != getOpCode()) {
      throw new IllegalArgumentException("Invalid operation: " + opCode);
    }
    this.timeStamp = buffer.getLong();
    this.isFirst = buffer.get();
    int keySize = buffer.getInt();
    buffer.limit(buffer.position() + keySize);
    ByteBuffer keyBlob = buffer.slice();
    buffer.position(buffer.limit());
    buffer.limit(buffer.capacity());

    int oldValueSize = buffer.getInt();
    buffer.limit(buffer.position() + oldValueSize);
    ByteBuffer oldValueBlob = buffer.slice();
    buffer.position(buffer.limit());
    buffer.limit(buffer.capacity());

    ByteBuffer valueBlob = buffer.slice();

    try {
      this.key = keySerializer.read(keyBlob);
      this.oldValue = valueSerializer.read(oldValueBlob);
      this.newValue = valueSerializer.read(valueBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
  }

  public K getKey() {
    return key;
  }

  V getOldValue() {
    return this.oldValue;
  }

  @Override
  public V getValue() {
    return this.newValue;
  }

  @Override
  public OperationCode getOpCode() {
    return REPLACE_CONDITIONAL;
  }

  @Override
  public Result<V> apply(Result<V> previousResult) {
    if(previousResult == null) {
      return null;
    } else {
      if(oldValue.equals(previousResult.getValue())) {
        return this;  // TODO: A new PutOperation can be created and returned here to minimize the size of returned operation
      } else {
        return previousResult;
      }
    }
  }

  @Override
  public ByteBuffer encode(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    ByteBuffer keyBuf = keySerializer.serialize(key);
    ByteBuffer oldValueBuf = valueSerializer.serialize(oldValue);
    ByteBuffer valueBuf = valueSerializer.serialize(newValue);

    ByteBuffer buffer = ByteBuffer.allocate(BYTE_SIZE_BYTES +   // Operation type
                                            INT_SIZE_BYTES +    // Size of the key payload
                                            LONG_SIZE_BYTES +   // Size of expiration time stamp
                                            BYTE_SIZE_BYTES +   // isFirst status bit
                                            keyBuf.remaining() + // the key payload itself
                                            INT_SIZE_BYTES +    // Size of the old value payload
                                            oldValueBuf.remaining() +  // The old value payload itself
                                            valueBuf.remaining());  // The value payload itself

    buffer.put(getOpCode().getValue());
    buffer.putLong(this.timeStamp);
    buffer.put(this.isFirst);
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
    return "{" + getOpCode() + "# key: " + key + ", oldValue: " + oldValue + ", newValue: " + newValue + "}";
  }

  @Override
  public boolean equals(final Object obj) {
    if(obj == null) {
      return false;
    }
    if(!(obj instanceof ConditionalReplaceOperation)) {
      return false;
    }

    ConditionalReplaceOperation<K, V> other = (ConditionalReplaceOperation)obj;
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
    hash = hash * 31 + oldValue.hashCode();
    hash = hash * 31 + newValue.hashCode();
    return hash;
  }

  @Override
  public long timeStamp() {
    //TODO: complete this when CAS is done
    return 0;
  }

  @Override
  public boolean isFirst() {
    if(isFirst == 1) {
      return true;
    } else {
      return false;
    }
  }
}
