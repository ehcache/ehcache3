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

package org.ehcache.impl.internal.store.offheap;

import org.ehcache.core.spi.store.AbstractValueHolder;
import org.ehcache.core.spi.store.Store;

import java.util.concurrent.TimeUnit;

/**
* OffHeapValueHolder
*/
public abstract class OffHeapValueHolder<V> extends AbstractValueHolder<V> {

  public OffHeapValueHolder(long id, long creationTime, long expireTime) {
    super(id, creationTime, expireTime);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || !(other instanceof OffHeapValueHolder<?>)) return false;

    OffHeapValueHolder<?> that = (OffHeapValueHolder<?>)other;

    if (!super.equals(that)) return false;
    return get().equals(that.get());

  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + get().hashCode();
    result = 31 * result + super.hashCode();
    return result;
  }

  abstract void updateMetadata(Store.ValueHolder<V> valueFlushed);

  abstract void writeBack();

  abstract void forceDeserialization();

  abstract void detach();
}
