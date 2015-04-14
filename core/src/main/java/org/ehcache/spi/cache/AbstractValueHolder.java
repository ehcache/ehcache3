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
    this(creationTime, NO_EXPIRE, creationTime);
  }

  protected AbstractValueHolder(long creationTime, long expirationTime) {
    this(creationTime, expirationTime, creationTime);
  }

  protected AbstractValueHolder(long creationTime, long expirationTime, long lastAccessTime) {
    this.creationTime = creationTime;
    this.lastAccessTime = lastAccessTime;
    this.expirationTime = expirationTime;
  }

  @Override
  public long creationTime(TimeUnit unit) {
    return TimeUnit.MILLISECONDS.convert(creationTime, unit);
  }

  @Override
  public void setExpirationTime(long expirationTime, TimeUnit unit) {
    if (expirationTime == NO_EXPIRE) {
      this.expirationTime = NO_EXPIRE;
    } else if (expirationTime <= 0) {
      throw new IllegalArgumentException("invalid expiration time: " + expirationTime);
    } else {
      this.expirationTime = TimeUnit.MILLISECONDS.convert(expirationTime, unit);;
    }
  }

  @Override
  public long expirationTime(TimeUnit unit) {
    return TimeUnit.MILLISECONDS.convert(expirationTime, unit);
  }

  @Override
  public boolean isExpired(long expirationTime, TimeUnit unit) {
    final long expire = this.expirationTime;
    if (expire == NO_EXPIRE) {
      return false;
    }

    if (expire <= unit.convert(expirationTime, TimeUnit.MILLISECONDS)) {
      return true;
    }

    return false;
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    return TimeUnit.MILLISECONDS.convert(lastAccessTime, unit);
  }

  @Override
  public void setLastAccessTime(long lastAccessTime, TimeUnit unit) {
    this.lastAccessTime = unit.convert(lastAccessTime, TimeUnit.MILLISECONDS);
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
      return other.hashCode() == hashCode() &&
          other.creationTime == creationTime &&
          other.expirationTime == expirationTime &&
          other.lastAccessTime == lastAccessTime;
    }
    return false;
  }

  @Override
  public String toString() {
    return "" + value();
  }
}
