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

import org.ehcache.clustered.client.internal.store.operations.BaseOperation;
import org.ehcache.clustered.client.internal.store.operations.OperationCode;
import org.ehcache.clustered.client.internal.store.operations.PutOperation;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

import static org.ehcache.clustered.client.internal.store.operations.OperationCode.PUT;
import static org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec.BYTE_SIZE_BYTES;
import static org.ehcache.clustered.client.internal.store.operations.codecs.OperationsCodec.INT_SIZE_BYTES;

public class PutOperationCodec<K, V> implements OperationCodec<K> {

  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;

  public PutOperationCodec(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public ByteBuffer encode(BaseOperation<K> operation) {
    if (!(operation instanceof PutOperation)) {
      throw new IllegalArgumentException(this.getClass().getName() + " can only decode " + PutOperation.class.getName());
    }
    PutOperation<K, V> putOperation = (PutOperation<K, V>)operation;

    K key = putOperation.getKey();
    V value = putOperation.getValue();

    ByteBuffer keyBuf = keySerializer.serialize(key);
    ByteBuffer valueBuf = valueSerializer.serialize(value);

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
    buffer.put(PUT.getValue());
    buffer.putInt(keyBuf.remaining());
    buffer.put(keyBuf);
    buffer.putInt(valueBuf.remaining());
    buffer.put(valueBuf);
    buffer.flip();
    return buffer;
  }

  public PutOperation<K, V> decode(ByteBuffer buffer) throws ClassNotFoundException {

    OperationCode opCode = OperationCode.valueOf(buffer.get());
    if (opCode != PUT) {
      throw new IllegalArgumentException(this.getClass().getName() + " can not decode operation of type " + opCode);
    }

    int keySize = buffer.getInt();
    buffer.limit(buffer.position() + keySize);
    ByteBuffer keyBlob = buffer.slice();
    buffer.position(buffer.limit());
    buffer.limit(buffer.capacity());
    int valueSize = buffer.getInt();
    buffer.limit(buffer.position() + valueSize);
    ByteBuffer valueBlob = buffer.slice();

    return new PutOperation<K, V>(
        keySerializer.read(keyBlob), valueSerializer.read(valueBlob));
  }
}
