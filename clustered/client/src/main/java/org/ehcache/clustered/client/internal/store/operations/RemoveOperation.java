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

public class RemoveOperation<K, V> extends BaseOperation<K, V> {

  public RemoveOperation(final K key) {
    super(key);
  }

  RemoveOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    super(buffer, keySerializer);
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
  public String toString() {
    return "{" + super.toString() + "}";
  }
}
