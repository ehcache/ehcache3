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

package org.ehcache.clustered.common.internal.store.operations.codecs;

import org.ehcache.clustered.common.internal.store.operations.Operation;
import org.ehcache.clustered.common.internal.store.operations.OperationCode;
import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

public class OperationsCodec<K, V> {

  private final Serializer<K> keySerializer;
  private final Serializer<V> valueSerializer;


  public OperationsCodec(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    this.keySerializer = keySerializer;
    this.valueSerializer = valueSerializer;
  }

  public ByteBuffer encode(Operation<K, V> operation) {
    return operation.encode(keySerializer, valueSerializer);
  }

  public static OperationCode getOperationCode(ByteBuffer buffer) {
    OperationCode opCode = OperationCode.valueOf(buffer.get());
    buffer.rewind();
    return opCode;
  }

  public Operation<K, V> decode(ByteBuffer buffer) {
    OperationCode opCode = getOperationCode(buffer);
    return opCode.decode(buffer, keySerializer, valueSerializer);
  }

  public Serializer<K> getKeySerializer() {
    return keySerializer;
  }

  public Serializer<V> getValueSerializer() {
    return valueSerializer;
  }
}
