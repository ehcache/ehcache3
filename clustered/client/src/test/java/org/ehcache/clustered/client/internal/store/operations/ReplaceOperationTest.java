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
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class ReplaceOperationTest extends BaseOperationTest {

  @Override
  protected <K, V> Operation<K, V> getNewOperation(final K key, final V value) {
    return new ReplaceOperation<K, V>(key, value);
  }

  @Override
  protected <K, V> Operation<K, V> getNewOperation(final ByteBuffer buffer, final Serializer<K> keySerializer, final Serializer<V> valueSerializer) {
    return new ReplaceOperation<K, V>(buffer, keySerializer, valueSerializer);
  }

  @Override
  protected OperationCode getOperationCode() {
    return OperationCode.REPLACE;
  }

  @Test
  public void testApply() throws Exception {
    ReplaceOperation<Long, String> operation = new ReplaceOperation<Long, String>(1L, "one");
    Operation<Long, String> applied = operation.apply(null);
    assertNull(applied);

    Operation<Long, String> anotherOperation = new PutOperation<Long, String>(1L, "another one");
    applied = operation.apply(anotherOperation);
    assertSame(operation, applied);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testApplyOnDifferentkey() throws Exception {
    ReplaceOperation<Long, String> operation = new ReplaceOperation<Long, String>(1L, "one");
    ReplaceOperation<Long, String> anotherOperation = new ReplaceOperation<Long, String>(2L, "two");
    operation.apply(anotherOperation);
  }
}