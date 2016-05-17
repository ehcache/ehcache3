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

package org.ehcache.clustered.client.internal.store.operations.codecs;

import org.ehcache.clustered.client.internal.store.operations.KeyValueOperation;
import org.ehcache.clustered.client.internal.store.operations.Operation;
import org.ehcache.clustered.client.internal.store.operations.OperationCode;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.serialization.SerializerException;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec.BYTE_SIZE_BYTES;
import static org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec.INT_SIZE_BYTES;

public abstract class BaseKeyValueOperationCodec<K, V> implements OperationCodec<K> {

  protected final Serializer<K> keySerializer;
  protected final Serializer<V> valueSerializer;

  public BaseKeyValueOperationCodec(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  protected abstract OperationCode getOperationCode();

  private void validateOperation(OperationCode code) {
    if (code != getOperationCode()) {
      throw new IllegalArgumentException(this.getClass().getName() +
                                         " can only encode/decode " + getOperationCode() + " operations");
    }
  }

  @Override
  public ByteBuffer encode(final Operation<K> operation) {
    validateOperation(operation.getOpCode());
    KeyValueOperation<K, V> kvOperation = (KeyValueOperation<K, V>)operation;
    ByteBuffer keyBuf = keySerializer.serialize(kvOperation.getKey());
    ByteBuffer valueBuf = valueSerializer.serialize(kvOperation.getValue());

    /**
     * Here we need to encode two objects of unknown size: the key and the value.
     * Encoding should be done in such a way that the key and value can be read
     * separately while decoding the bytes.
     * So the way it is done here is by writing the size of the payload along with
     * the payload. That is, the size of the key payload is written before the key
     * itself and the same for value.
     *
     * While decoding, the size is read first and then reading the same number of
     * bytes will get you the key payload. Same for value.
     */
    ByteBuffer buffer = ByteBuffer.allocate(BYTE_SIZE_BYTES +   // Operation type
                                            INT_SIZE_BYTES +    // Size of the key payload
                                            keyBuf.remaining() +  // the kay payload itself
                                            INT_SIZE_BYTES +    // Size of value payload
                                            valueBuf.remaining());  // The value payload itself

    buffer.put(operation.getOpCode().getValue());
    buffer.putInt(keyBuf.remaining());
    buffer.put(keyBuf);
    buffer.putInt(valueBuf.remaining());
    buffer.put(valueBuf);

    buffer.flip();
    return buffer;
  }

  protected abstract KeyValueOperation<K, V> newOperation(K key, V value);

  @Override
  public Operation<K> decode(final ByteBuffer buffer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    validateOperation(opCode);
    int keySize = buffer.getInt();
    buffer.limit(buffer.position() + keySize);
    ByteBuffer keyBlob = buffer.slice();

    buffer.position(buffer.limit());
    buffer.limit(buffer.capacity());
    int valueSize = buffer.getInt();
    buffer.limit(buffer.position() + valueSize);
    ByteBuffer valueBlob = buffer.slice();

    try {
      return newOperation(keySerializer.read(keyBlob), valueSerializer.read(valueBlob));
    } catch (ClassNotFoundException e) {
      throw new SerializerException(e);
    }
  }
}
