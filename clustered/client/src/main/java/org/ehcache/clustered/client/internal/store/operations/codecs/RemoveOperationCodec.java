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

import org.ehcache.clustered.client.internal.store.operations.Operation;
import org.ehcache.clustered.client.internal.store.operations.OperationCode;
import org.ehcache.clustered.client.internal.store.operations.RemoveOperation;
import org.ehcache.spi.serialization.Serializer;

import static org.ehcache.clustered.client.internal.store.operations.OperationCode.REMOVE;

public class RemoveOperationCodec<K> extends BaseOperationCodec<K> {
  public RemoveOperationCodec(final Serializer<K> keySerializer) {
    super(keySerializer);
  }

  @Override
  protected OperationCode getOperationCode() {
    return REMOVE;
  }

  @Override
  protected Operation<K> newOperation(final K key) {
    return new RemoveOperation<K>(key);
  }
}
