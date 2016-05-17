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

public class RemoveOperation<K> extends BaseOperation<K> {

  public RemoveOperation(final K key) {
    super(key);
  }

  @Override
  public OperationCode getOpCode() {
    return OperationCode.REMOVE;
  }

  @Override
  public Operation<K> apply(final Operation<K> kOperation) {
    return null;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
