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

/**
 * Base class that represents a {@link org.ehcache.Cache} operation
 *
 * @param <K> key type
 */
public abstract class BaseOperation<K, V> implements Operation<K, V> {

  public static final int BYTE_SIZE_BYTES = 1;
  public static final int INT_SIZE_BYTES = 4;
  public static final int LONG_SIZE_BYTES = 8;

  protected K key;
  protected V value;

  public BaseOperation() {
    this.key = null;
    this.value = null;
  }

  public BaseOperation(final K key) {
    this(key, null);
  }

  public BaseOperation(final K key, final V value) {
    if(key == null) {
      throw new NullPointerException("Key can not be null");
    }
    checkValueNonNull(value);
    this.key = key;
    this.value = value;
  }

  protected void checkValueNonNull(V value) {
    if(value == null) {
      throw new NullPointerException("Value can not be null");
    }
  }

  BaseOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    validateOperation(opCode);
    int keySize = buffer.getInt();
    buffer.limit(buffer.position() + keySize);
    ByteBuffer keyBlob = buffer.slice();

    buffer.position(buffer.limit());
    buffer.limit(buffer.capacity());
    ByteBuffer valueBlob = buffer.slice();

    try {
      this.key = keySerializer.read(keyBlob);
      this.value = valueSerializer.read(valueBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return this.value;
  }

  /**
   * Here we need to encode two objects of unknown size: the key and the value.
   * Encoding should be done in such a way that the key and value can be read
   * separately while decoding the bytes.
   * So the way it is done here is by writing the size of the payload along with
   * the payload. That is, the size of the key payload is written before the key
   * itself. The value payload is written after that.
   *
   * While decoding, the size is read first and then reading the same number of
   * bytes will get you the key payload. Whatever that is left is the value payload.
   */
  @Override
  public ByteBuffer encode(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    ByteBuffer keyBuf = keySerializer.serialize(key);
    ByteBuffer valueBuf = null;

    int size = BYTE_SIZE_BYTES +   // Operation type
               INT_SIZE_BYTES +    // Size of the key payload
               keyBuf.remaining();   // the kay payload itself

    if(value != null) {
      valueBuf = valueSerializer.serialize(value);
      size += valueBuf.remaining();
    }

    ByteBuffer buffer = ByteBuffer.allocate(size);

    buffer.put(getOpCode().getValue());
    buffer.putInt(keyBuf.remaining());
    buffer.put(keyBuf);
    if(valueBuf != null) {
      buffer.put(valueBuf);
    }

    buffer.flip();
    return buffer;
  }

  protected void validateOperation(OperationCode code) {
    if (code != getOpCode()) {
      throw new IllegalArgumentException("Invalid operation: " + code);
    }
  }

  protected void assertSameKey(Operation<K, V> operation) {
    if(!key.equals(operation.getKey())) {
      throw new IllegalArgumentException(this + " can not be applied on top of " + operation + " as the keys do not match");
    }
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("{");
    builder.append(getOpCode());
    builder.append("# key: ");
    builder.append(key);
    if(value != null) {
      builder.append(", value: ");
      builder.append(value);
    }
    builder.append("}");

    return builder.toString();
  }

  @Override
  public boolean equals(final Object obj) {
    if(obj == null) {
      return false;
    }
    if(!(obj instanceof BaseOperation)) {
      return false;
    }

    BaseOperation<K, V> other = (BaseOperation)obj;
    if(this.getOpCode() != other.getOpCode()) {
      return false;
    }
    if(!this.getKey().equals(other.getKey())) {
      return false;
    }
    if(this.getValue() == null && other.getValue() != null) {
      return false;
    }
    if(!this.getValue().equals(other.getValue())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return getOpCode().hashCode() +
           (key == null ? 0: key.hashCode()) +
           (value == null? 0: value.hashCode());
  }
}
