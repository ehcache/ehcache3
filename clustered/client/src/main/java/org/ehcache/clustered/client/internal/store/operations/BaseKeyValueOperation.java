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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.ehcache.clustered.client.internal.store.operations.codecs.CodecException;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

public abstract class BaseKeyValueOperation<K, V> extends BaseOperation<K, V> {

  protected final V value;

  BaseKeyValueOperation(K key, V value) {
    super(key);
    if(value == null) {
      throw new NullPointerException("Value can not be null");
    }
    this.value = value;
  }

  BaseKeyValueOperation(ByteBuffer buffer, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
    super(buffer, keySerializer, valueSerializer);
    try {
      value = valueSerializer.read(buffer.slice());
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
  }

  public V getValue() {
    return value;
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
    ByteBuffer valueBuf = valueSerializer.serialize(value);

    int size = BYTE_SIZE_BYTES +   // Operation type
               INT_SIZE_BYTES +    // Size of the key payload
               keyBuf.remaining() + // the kay payload itself
               valueBuf.remaining();  // the value payload

    ByteBuffer buffer = ByteBuffer.allocate(size);

    buffer.put(getOpCode().getValue());
    buffer.putInt(keyBuf.remaining());
    buffer.put(keyBuf);
    buffer.put(valueBuf);

    buffer.flip();
    return buffer;
  }

  @Override
  public String toString() {
    return super.toString() + ", value: " + value;
  }

  @SuppressFBWarnings("NP_NULL_ON_SOME_PATH")
  @Override
  public boolean equals(final Object obj) {
    if(!super.equals(obj)) {
      return false;
    }
    if(!(obj instanceof BaseKeyValueOperation)) {
      return false;
    }

    BaseKeyValueOperation other = (BaseKeyValueOperation) obj;
    if(!this.getValue().equals(other.getValue())) {
      return false;
    }
    return true;
  }

  @Override
  public int hashCode() {
    return super.hashCode() +
           (value == null? 0: value.hashCode());
  }
}
