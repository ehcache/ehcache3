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

  protected final K key;

  public BaseOperation(final K key) {
    if(key == null) {
      throw new NullPointerException("Key can not be null");
    }
    this.key = key;
  }

  BaseOperation(final ByteBuffer buffer, final Serializer<K> keySerializer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    validateOperation(opCode);
    ByteBuffer keyBlob = buffer.slice();
    try {
      this.key = keySerializer.read(keyBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
  }

  BaseOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    validateOperation(opCode);
    int keySize = buffer.getInt();
    buffer.limit(buffer.position() + keySize);
    ByteBuffer keyBlob = buffer.slice();
    try {
      this.key = keySerializer.read(keyBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
    buffer.position(buffer.limit());
    buffer.limit(buffer.capacity());
  }

  public K getKey() {
    return key;
  }

  @Override
  public ByteBuffer encode(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    ByteBuffer keyBuf = keySerializer.serialize(key);

    int size = BYTE_SIZE_BYTES +   // Operation type
               keyBuf.remaining();   // the kay payload itself

    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.put(getOpCode().getValue());
    buffer.put(keyBuf);
    buffer.flip();
    return buffer;
  }

  protected void validateOperation(OperationCode code) {
    if (code != getOpCode()) {
      throw new IllegalArgumentException("Invalid operation: " + code);
    }
  }

  @Override
  public String toString() {
    return getOpCode() + "# key: " + key;
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
    return true;
  }

  @Override
  public int hashCode() {
    return getOpCode().hashCode() +
           (key == null ? 0: key.hashCode());
  }
}
