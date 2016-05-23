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

public class ConditionalReplaceOperation<K, V> extends BaseOperation<K, V> {

  private final V oldValue;

  public ConditionalReplaceOperation(final K key, final V oldValue, final V value) {
    super(key, value);
    if(oldValue == null) {
      throw new NullPointerException("Old value can not be null");
    }
    this.oldValue = oldValue;
  }

  ConditionalReplaceOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    validateOperation(opCode);

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
      this.value = valueSerializer.read(valueBlob);
      this.oldValue = valueSerializer.read(oldValueBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
  }

  public V getOldValue() {
    return this.oldValue;
  }

  @Override
  public OperationCode getOpCode() {
    return REPLACE_CONDITIONAL;
  }

  @Override
  public Operation<K, V> apply(final Operation<K, V> previousOperation) {
    if(previousOperation == null) {
      return null;
    } else {
      assertSameKey(previousOperation);
      if(oldValue.equals(previousOperation.getValue())) {
        return this;  // TODO: A new PutOperation can be created and returned here to minimize the size of returned operation
      } else {
        return previousOperation;
      }
    }
  }

  @Override
  public ByteBuffer encode(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    ByteBuffer keyBuf = keySerializer.serialize(key);
    ByteBuffer valueBuf = valueSerializer.serialize(value);
    ByteBuffer oldValueBuf = valueSerializer.serialize(oldValue);

    ByteBuffer buffer = ByteBuffer.allocate(BYTE_SIZE_BYTES +   // Operation type
                                            INT_SIZE_BYTES +    // Size of the key payload
                                            keyBuf.remaining() +  // the kay payload itself
                                            INT_SIZE_BYTES +    // Size of old value payload
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
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append(getOpCode());
    builder.append("# key: ");
    builder.append(key);
    if(oldValue != null) {
      builder.append(", oldValue: ");
      builder.append(oldValue);
    }
    if(value != null) {
      builder.append(", newvalue: ");
      builder.append(value);
    }
    builder.append("}");

    return builder.toString();
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
