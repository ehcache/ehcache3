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

public class ConditionalRemoveOperationTest extends BaseKeyValueOperationTest {

  @Override
  protected <K, V> BaseKeyValueOperation<K, V> getNewOperation(final K key, final V value, long timestamp) {
    return new ConditionalRemoveOperation<>(key, value, timestamp);
  }

  @Override
  protected <K, V> BaseKeyValueOperation<K, V> getNewOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    return new ConditionalRemoveOperation<>(buffer, keySerializer, valueSerializer);
  }

  @Override
  protected OperationCode getOperationCode() {
    return OperationCode.REMOVE_CONDITIONAL;
  }

  @Test
  public void testApply() throws Exception {
    ConditionalRemoveOperation<Long, String> operation = new ConditionalRemoveOperation<>(1L, "one", System.currentTimeMillis());
    Result<Long, String> result = operation.apply(null);
    assertNull(result);

    PutOperation<Long, String> anotherOperation = new PutOperation<>(1L, "one", System.currentTimeMillis());
    result = operation.apply(anotherOperation);
    assertNull(result);

    PutIfAbsentOperation<Long, String> yetAnotherOperation = new PutIfAbsentOperation<>(1L, "two", System.currentTimeMillis());
    result = operation.apply(yetAnotherOperation);
    assertSame(yetAnotherOperation, result);
  }
}
