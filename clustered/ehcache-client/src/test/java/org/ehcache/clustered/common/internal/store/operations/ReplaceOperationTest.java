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
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ReplaceOperationTest extends BaseKeyValueOperationTest {

  @Override
  protected <K, V> BaseKeyValueOperation<K, V> getNewOperation(final K key, final V value, long timestamp) {
    return new ReplaceOperation<>(key, value, timestamp);
  }

  @Override
  protected <K, V> BaseKeyValueOperation<K, V> getNewOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    return new ReplaceOperation<>(buffer, keySerializer, valueSerializer);
  }

  @Override
  protected OperationCode getOperationCode() {
    return OperationCode.REPLACE;
  }

  @Test
  public void testApply() throws Exception {
    ReplaceOperation<Long, String> operation = new ReplaceOperation<>(1L, "one", System.currentTimeMillis());
    Result<Long, String> result = operation.apply(null);
    assertNull(result);

    PutOperation<Long, String> anotherOperation = new PutOperation<>(1L, "another one", System.currentTimeMillis());
    result = operation.apply(anotherOperation);
    assertSame(operation, result);
  }
}
