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

import org.ehcache.impl.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.spi.cache.AbstractValueHolder;
import org.ehcache.spi.cache.Store;
import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.util.concurrent.TimeUnit;

/**
* OffHeapValueHolder
*/
public final class OffHeapValueHolder<V> extends AbstractValueHolder<V> {

  public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  private final V value;
  private final WriteContext writeContext;

  public OffHeapValueHolder(long id, V value, long creationTime, long expireTime) {
    this(id, value, creationTime, expireTime, 0, 0, null);
  }

  public OffHeapValueHolder(long id, V value, long creationTime, long expireTime, long lastAccessTime, long hits, WriteContext writeContext) {
    super(id, creationTime, expireTime);
    setLastAccessTime(lastAccessTime, TIME_UNIT);
    this.value = value;
    this.setHits(hits);
    this.writeContext = writeContext;
  }

  @Override
  public V value() {
    return value;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    OffHeapValueHolder that = (OffHeapValueHolder)other;

    if (!super.equals(that)) return false;
    if (!value.equals(that.value)) return false;

    return true;
  }

  @Override
  final protected TimeUnit nativeTimeUnit() {
    return TIME_UNIT;
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + value.hashCode();
    result = 31 * result + super.hashCode();
    return result;
  }

  public void writeBack() {
    writeContext.setLong(OffHeapValueHolderPortability.ACCESS_TIME_OFFSET, lastAccessTime(TimeUnit.MILLISECONDS));
    writeContext.setLong(OffHeapValueHolderPortability.EXPIRE_TIME_OFFSET, expirationTime(TimeUnit.MILLISECONDS));
    writeContext.setLong(OffHeapValueHolderPortability.HITS_OFFSET, hits());
    writeContext.flush();
  }

  public void updateMetadata(final Store.ValueHolder<V> valueFlushed) {
    if(getId() != valueFlushed.getId()) {
      throw new IllegalArgumentException("Wrong id passed in [this.id != id] : " + getId() + " != " + valueFlushed.getId());
    }
    this.setLastAccessTime(valueFlushed.lastAccessTime(OffHeapValueHolder.TIME_UNIT), OffHeapValueHolder.TIME_UNIT);
    this.setExpirationTime(valueFlushed.expirationTime(OffHeapValueHolder.TIME_UNIT), OffHeapValueHolder.TIME_UNIT);
    this.setHits(valueFlushed.hits());
  }
}
