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

public interface Operation<K, V> {

  int BYTE_SIZE_BYTES = 1;
  int INT_SIZE_BYTES = 4;
  int LONG_SIZE_BYTES = 8;

  OperationCode getOpCode();

  K getKey();

  Result<K, V> apply(Result<K, V> previousResult);

  ByteBuffer encode(Serializer<K> keySerializer, Serializer<V> valueSerializer);

  /**
   * Time when the operation occurred
   */
  long timeStamp();

  /**
   * Does the value installed by this operation have a specific expiry time
   */
  boolean isExpiryAvailable();

  /**
   * Time when the operations installed value expires
   */
  long expirationTime();

}
