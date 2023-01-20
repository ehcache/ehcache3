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

import org.ehcache.impl.internal.store.heap.holders.BaseOnHeapKey;
import org.ehcache.impl.internal.store.heap.holders.OnHeapKey;
import org.ehcache.spi.copy.Copier;

/**
 * @author Albin Suresh
 */
public class CopiedOnHeapKey<K> extends BaseOnHeapKey<K> {
  private final K copiedKey;
  private final Copier<K> keyCopier;

  public CopiedOnHeapKey(final K actualKeyObject, final Copier<K> keyCopier) {
    super(actualKeyObject.hashCode());
    this.keyCopier = keyCopier;
    this.copiedKey = keyCopier.copyForWrite(actualKeyObject);
  }

  @Override
  public K getActualKeyObject() {
    return keyCopier.copyForRead(copiedKey);
  }

  K getCopiedKey() {
    return copiedKey;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof CopiedOnHeapKey) {
      return copiedKey.equals(((CopiedOnHeapKey)other).copiedKey);
    } else if (other instanceof OnHeapKey) {
      return copiedKey.equals(((OnHeapKey)other).getActualKeyObject());
    }
    return false;
  }
}
