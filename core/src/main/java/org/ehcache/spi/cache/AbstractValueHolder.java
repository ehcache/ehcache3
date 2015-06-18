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
package org.ehcache.spi.cache;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

/**
 * @author Ludovic Orban
 */
public abstract class AbstractValueHolder<V> implements Store.ValueHolder<V>, Serializable {

  private final long creationTime;
  private volatile long lastAccessTime;
  private volatile long expirationTime;

  protected AbstractValueHolder(long creationTime) {
    this(creationTime, NO_EXPIRE);
  }

  protected AbstractValueHolder(long creationTime, long expirationTime) {
    this.creationTime = creationTime;
    this.expirationTime = expirationTime;
    this.lastAccessTime = creationTime;
  }

  protected abstract TimeUnit nativeTimeUnit();

  @Override
  public long creationTime(TimeUnit unit) {
    return unit.convert(creationTime, nativeTimeUnit());
  }

  public void setExpirationTime(long expirationTime, TimeUnit unit) {
    if (expirationTime == NO_EXPIRE) {
      this.expirationTime = NO_EXPIRE;
    } else if (expirationTime <= 0) {
      throw new IllegalArgumentException("invalid expiration time: " + expirationTime);
    } else {
      this.expirationTime = nativeTimeUnit().convert(expirationTime, unit);;
    }
  }

  @Override
  public long expirationTime(TimeUnit unit) {
    final long expire = this.expirationTime;
    if (expire == NO_EXPIRE) {
      return NO_EXPIRE;
    }
    return unit.convert(expire, nativeTimeUnit());
  }

  @Override
  public boolean isExpired(long expirationTime, TimeUnit unit) {
    final long expire = this.expirationTime;
    if (expire == NO_EXPIRE) {
      return false;
    }
    return expire <= nativeTimeUnit().convert(expirationTime, unit);
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    return unit.convert(lastAccessTime, nativeTimeUnit());
  }

  public void setLastAccessTime(long lastAccessTime, TimeUnit unit) {
    this.lastAccessTime = unit.convert(lastAccessTime, nativeTimeUnit());
  }

  @Override
  public int hashCode() {
    int result = 1;
    result = 31 * result + (int)(creationTime ^ (creationTime >>> 32));
    result = 31 * result + (int)(lastAccessTime ^ (lastAccessTime >>> 32));
    result = 31 * result + (int)(expirationTime ^ (expirationTime >>> 32));
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof AbstractValueHolder) {
      AbstractValueHolder<?> other = (AbstractValueHolder<?>) obj;
      return
          other.creationTime(nativeTimeUnit()) == creationTime && creationTime(other.nativeTimeUnit()) == other.creationTime &&
          other.expirationTime(nativeTimeUnit()) == expirationTime && expirationTime(other.nativeTimeUnit()) == other.expirationTime &&
          other.lastAccessTime(nativeTimeUnit()) == lastAccessTime && lastAccessTime(other.nativeTimeUnit()) == other.lastAccessTime;
    }
    return false;
  }

  @Override
  public float hitRate(TimeUnit unit) {
    //XXX
    return 0.0f;
  }

  @Override
  public String toString() {
    return "" + value();
  }
}
