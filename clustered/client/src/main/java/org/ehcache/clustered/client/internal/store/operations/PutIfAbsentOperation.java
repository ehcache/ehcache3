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

import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

public class PutIfAbsentOperation<K, V> extends BaseKeyValueOperation<K, V> implements Result<V> {

  public PutIfAbsentOperation(final K key, final V value, final long timeStamp) {
    super(key, value, timeStamp);
  }

  PutIfAbsentOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    super(buffer, keySerializer, valueSerializer);
  }

  @Override
  public OperationCode getOpCode() {
    return OperationCode.PUT_IF_ABSENT;
  }

  /**
   * PutIfAbsent operation succeeds only when there is no previous operation
   * for the same key.
   */
  @Override
  public Result<V> apply(final Result<V> previousOperation) {
    if(previousOperation == null) {
      return this;
    } else {
      return previousOperation;
    }
  }
}
