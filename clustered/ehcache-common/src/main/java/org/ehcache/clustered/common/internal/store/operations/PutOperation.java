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

import org.ehcache.spi.serialization.Serializer;

import java.nio.ByteBuffer;

/**
 * Represents the PUT operation on a {@link org.ehcache.Cache}
 * @param <K> key type
 * @param <V> value type
 */
public class PutOperation<K, V> extends BaseKeyValueOperation<K, V> implements Result<K, V> {

  public PutOperation(final K key, final V value, final long timeStamp) {
    super(key, value, timeStamp);
  }

  PutOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    super(buffer, keySerializer, valueSerializer);
  }

  PutOperation(final BaseKeyValueOperation<K, V> copy, final long timeStamp) {
    super(copy, timeStamp);
  }

  @Override
  public OperationCode getOpCode() {
    return OperationCode.PUT;
  }

  /**
   * Put operation applied on top of another {@link Operation} does not care
   * what the other operation is. The result is gonna be {@code this} operation.
   */
  @Override
  public Result<K, V> apply(final Result<K, V> previousOperation) {
    return this;
  }

  @Override
  public PutOperation<K, V> asOperationExpiringAt(long expirationTime) {
    if (isExpiryAvailable() && expirationTime == expirationTime()) {
      return this;
    } else {
      return new PutOperation<>(this, -expirationTime);
    }
  }
}
