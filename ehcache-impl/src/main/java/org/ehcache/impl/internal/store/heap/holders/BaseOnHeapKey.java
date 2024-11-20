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

package org.ehcache.impl.internal.store.heap.holders;

import org.ehcache.impl.internal.store.heap.holders.OnHeapKey;

abstract class BaseOnHeapKey<K> implements OnHeapKey<K> {

  private final int hashCode;

  BaseOnHeapKey(int hashCode) {
    this.hashCode = hashCode;
  }

  @Override
  public int hashCode() {
    return hashCode;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof OnHeapKey) {
      OnHeapKey<?> other = (OnHeapKey<?>) obj;
      return getActualKeyObject().equals(other.getActualKeyObject());
    }

    return false;
  }

}
