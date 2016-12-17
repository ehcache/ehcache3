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

public class RemoveOperation<K, V> implements Operation<K, V> {

  private final K key;
  private final long timeStamp;

  public RemoveOperation(final K key, final long timeStamp) {
    if(key == null) {
      throw new NullPointerException("Key can not be null");
    }
    this.key = key;
    this.timeStamp = timeStamp;
  }

  RemoveOperation(final ByteBuffer buffer, final Serializer<K> keySerializer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    if (opCode != getOpCode()) {
      throw new IllegalArgumentException("Invalid operation: " + opCode);
    }
    this.timeStamp = buffer.getLong();
    ByteBuffer keyBlob = buffer.slice();
    try {
      this.key = keySerializer.read(keyBlob);
    } catch (ClassNotFoundException e) {
      throw new CodecException(e);
    }
  }

  public K getKey() {
    return key;
  }

  @Override
  public OperationCode getOpCode() {
    return OperationCode.REMOVE;
  }

  /**
   * Remove operation applied on top of another operation does not care
   * what the other operation is. The result is always gonna be null.
   */
  @Override
  public Result<V> apply(final Result<V> previousOperation) {
    return null;
  }

  @Override
  public ByteBuffer encode(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    ByteBuffer keyBuf = keySerializer.serialize(key);

    int size = BYTE_SIZE_BYTES +   // Operation type
               LONG_SIZE_BYTES +   // Size of expiration time stamp
               keyBuf.remaining();   // the key payload itself

    ByteBuffer buffer = ByteBuffer.allocate(size);
    buffer.put(getOpCode().getValue());
    buffer.putLong(this.timeStamp);
    buffer.put(keyBuf);
    buffer.flip();
    return buffer;
  }

  @Override
  public String toString() {
    return "{" + getOpCode() + "# key: " + key + "}";
  }

  @Override
  public boolean equals(final Object obj) {
    if(obj == null) {
      return false;
    }
    if(!(obj instanceof RemoveOperation)) {
      return false;
    }

    @SuppressWarnings("unchecked")
    RemoveOperation<K, V> other = (RemoveOperation) obj;
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
    int hash = getOpCode().hashCode();
    hash = hash * 31 + key.hashCode();
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
