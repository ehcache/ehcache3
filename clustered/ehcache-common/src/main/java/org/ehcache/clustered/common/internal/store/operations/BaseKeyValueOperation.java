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

abstract class BaseKeyValueOperation<K, V> implements Operation<K, V> {

  private final K key;
  private final LazyValueHolder<V> valueHolder;

  //-ve values are expiry times
  //+ve values are operation timestamps
  private final long timeStamp;

  BaseKeyValueOperation(K key, V value, long timeStamp) {
    if(key == null) {
      throw new NullPointerException("Key can not be null");
    }
    if(value == null) {
      throw new NullPointerException("Value can not be null");
    }
    this.key = key;
    this.valueHolder = new LazyValueHolder<>(value);
    this.timeStamp = timeStamp;
  }

  BaseKeyValueOperation(BaseKeyValueOperation<K, V> copy, long timeStamp) {
    this.key = copy.key;
    this.valueHolder = copy.valueHolder;
    this.timeStamp = timeStamp;
  }

  BaseKeyValueOperation(ByteBuffer buffer, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
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
    try {
      this.key = keySerializer.read(keyBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
    this.valueHolder = new LazyValueHolder<>(buffer.slice(), valueSerializer);
  }

  public K getKey() {
    return key;
  }

  public V getValue() {
    return valueHolder.getValue();
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
    ByteBuffer valueBuf = valueHolder.encode(valueSerializer);

    int size = BYTE_SIZE_BYTES +   // Operation type
               INT_SIZE_BYTES +    // Size of the key payload
               LONG_SIZE_BYTES +   // Size of expiration time stamp
               keyBuf.remaining() + // the key payload itself
               valueBuf.remaining();  // the value payload

    ByteBuffer buffer = ByteBuffer.allocate(size);

    buffer.put(getOpCode().getValue());
    buffer.putLong(this.timeStamp);
    buffer.putInt(keyBuf.remaining());
    buffer.put(keyBuf);
    buffer.put(valueBuf);
    buffer.flip();
    return buffer;
  }

  @Override
  public String toString() {
    return "{" + getOpCode() + "# key: " + key + ", value: " + getValue() + "}";
  }

  @Override
  public boolean equals(final Object obj) {
    if(obj == null) {
      return false;
    }
    if(!(obj instanceof BaseKeyValueOperation)) {
      return false;
    }

    BaseKeyValueOperation<?, ?> other = (BaseKeyValueOperation<?, ?>) obj;
    if(this.getOpCode() != other.getOpCode()) {
      return false;
    }
    if(!this.getKey().equals(other.getKey())) {
      return false;
    }
    if(!this.getValue().equals(other.getValue())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    int hash = getOpCode().hashCode();
    hash = hash * 31 + key.hashCode();
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
