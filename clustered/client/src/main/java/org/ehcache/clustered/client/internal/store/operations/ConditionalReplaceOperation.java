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

public class ConditionalReplaceOperation<K, V> extends BaseOperation<K, V> implements Result<V> {

  private final V oldValue;
  private final V newValue;

  public ConditionalReplaceOperation(final K key, final V oldValue, final V newValue) {
    super(key);
    if(oldValue == null) {
      throw new NullPointerException("Old value can not be null");
    }
    this.oldValue = oldValue;
    if(newValue == null) {
      throw new NullPointerException("New value can not be null");
    }
    this.newValue = newValue;
  }

  ConditionalReplaceOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    super(buffer, keySerializer, valueSerializer);

    int oldValueSize = buffer.getInt();
    buffer.limit(buffer.position() + oldValueSize);
    ByteBuffer oldValueBlob = buffer.slice();
    buffer.position(buffer.limit());
    buffer.limit(buffer.capacity());

    ByteBuffer valueBlob = buffer.slice();

    try {
      this.oldValue = valueSerializer.read(oldValueBlob);
      this.newValue = valueSerializer.read(valueBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
  }

  public V getOldValue() {
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
                                            keyBuf.remaining() + // the key payload itself
                                            INT_SIZE_BYTES +    // Size of the value payload
                                            oldValueBuf.remaining() +  // The old value payload itself
                                            valueBuf.remaining());  // The value payload itself

    buffer.put(getOpCode().getValue());
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
    return "{" + super.toString() + ", oldValue: " + oldValue + ", newValue: " + newValue + "}";
  }

  @Override
  public boolean equals(final Object obj) {
    if(!super.equals(obj)) {
      return false;
    }

    if(!(obj instanceof ConditionalReplaceOperation)) {
      return false;
    }

    ConditionalReplaceOperation<K, V> other = (ConditionalReplaceOperation)obj;
    if(this.getOldValue() == null && other.getOldValue() != null) {
      return false;
    }
    if(!this.getOldValue().equals(other.getOldValue())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode() + (oldValue == null? 0: oldValue.hashCode());
  }
}
