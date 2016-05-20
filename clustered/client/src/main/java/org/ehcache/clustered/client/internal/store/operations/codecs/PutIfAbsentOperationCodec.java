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
import org.ehcache.clustered.client.internal.store.operations.OperationCode;
import org.ehcache.clustered.client.internal.store.operations.PutIfAbsentOperation;
import org.ehcache.spi.serialization.Serializer;

import static org.ehcache.clustered.client.internal.store.operations.OperationCode.PUT_IF_ABSENT;

public class PutIfAbsentOperationCodec<K, V>  extends BaseKeyValueOperationCodec<K, V> {

  public PutIfAbsentOperationCodec(final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    super(keySerializer, valueSerializer);
  }

  @Override
  protected KeyValueOperation<K, V> newOperation(final K key, final V value) {
    return new PutIfAbsentOperation<K, V>(key, value);
  }

  @Override
  protected OperationCode getOperationCode() {
    return PUT_IF_ABSENT;
  }
}
