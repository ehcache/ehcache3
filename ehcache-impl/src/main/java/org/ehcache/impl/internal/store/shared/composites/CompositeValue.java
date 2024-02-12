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

package org.ehcache.impl.internal.store.shared.composites;

import java.util.Objects;

public class CompositeValue<T> {
  private final int storeId;
  private final T value;

  public CompositeValue(int storeId, T value) {
    this.storeId = storeId;
    this.value = value;
  }

  public int getStoreId() {
    return storeId;
  }

  public T getValue() {
    return value;
  }

  @Override
  public int hashCode() {
    return mergeHashes(storeId, value.hashCode());
  }

  public static int mergeHashes(int hashA, int hashB) {
    return hashA * 31 + hashB;
  }

  public static int unmergeHashB(int merged, int hashA) {
    return merged - (hashA * 31);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    CompositeValue<?> that = (CompositeValue<?>) o;
    return storeId == that.storeId && Objects.equals(value, that.value);
  }
}
