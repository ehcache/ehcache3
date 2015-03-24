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

package org.ehcache.internal.store.offheap;

import org.ehcache.internal.store.offheap.portability.OffHeapValueHolderPortability;
import org.ehcache.spi.cache.Store;

import org.terracotta.offheapstore.storage.portability.WriteContext;

import java.util.concurrent.TimeUnit;

/**
* OffHeapValueHolder
*/
public final class OffHeapValueHolder<V> implements Store.ValueHolder<V> {

  public static final long NO_EXPIRE = -1;

  private final V value;
  private final WriteContext writeContext;
  private final long creationTime;

  private long lastAccessTime;
  private long expireTime;

  public OffHeapValueHolder(V value, long creationTime, long expireTime) {
    this(value, creationTime, expireTime, 0, null);
  }

  public OffHeapValueHolder(V value, long creationTime, long expireTime, long lastAccessTime, WriteContext writeContext) {
    this.value = value;
    this.creationTime = creationTime;
    this.expireTime = expireTime;
    this.lastAccessTime = lastAccessTime;
    this.writeContext = writeContext;
  }

  @Override
  public V value() {
    return value;
  }

  @Override
  public long creationTime(TimeUnit unit) {
    return TimeUnit.MILLISECONDS.convert(creationTime, unit);
  }

  public long expireTime(TimeUnit unit) {
    return TimeUnit.MILLISECONDS.convert(expireTime, unit);
  }

  void setExpireTimeMillis(long expireTime) {
    this.expireTime = expireTime;
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    return TimeUnit.MILLISECONDS.convert(lastAccessTime, unit);
  }

  void setLastAccessTimeMillis(long accessTime) {
    this.lastAccessTime = accessTime;
  }

  @Override
  public float hitRate(TimeUnit unit) {
    throw new UnsupportedOperationException("TODO Implement me!");
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    OffHeapValueHolder that = (OffHeapValueHolder)other;

    if (creationTime != that.creationTime) return false;
    if (expireTime != that.expireTime) return false;
    if (lastAccessTime != that.lastAccessTime) return false;
    if (!value.equals(that.value)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = value.hashCode();
    result = 31 * result + (int)(creationTime ^ (creationTime >>> 32));
    result = 31 * result + (int)(lastAccessTime ^ (lastAccessTime >>> 32));
    result = 31 * result + (int)(expireTime ^ (expireTime >>> 32));
    return result;
  }

  public boolean isExpired(long now) {
    final long expire = expireTime;

    if (expire == NO_EXPIRE) {
      return false;
    }

    return expire <= now;
  }

  void writeBack() {
    writeContext.setLong(OffHeapValueHolderPortability.ACCESS_TIME_OFFSET, lastAccessTime);
    writeContext.setLong(OffHeapValueHolderPortability.EXPIRE_TIME_OFFSET, expireTime);
  }
}
