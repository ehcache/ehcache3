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
package org.ehcache.internal.store;

import java.util.concurrent.TimeUnit;

class TimeStampedOnHeapValueHolder<V> implements OnHeapValueHolder<V> {

  static final long NO_EXPIRE = -1;

  private final OnHeapValueHolder<V> delegate;

  private volatile long expireTime;

  TimeStampedOnHeapValueHolder(OnHeapValueHolder<V> delegate, long expireTime) {
    if (delegate == null) {
      throw new NullPointerException("null delegate");
    }
    this.delegate = delegate;

    setExpireTimeMillis(expireTime);
    this.expireTime = expireTime;
  }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (!(o instanceof TimeStampedOnHeapValueHolder)) return false;
    return delegate.equals(((TimeStampedOnHeapValueHolder) o).delegate);
  }

  @Override
  public int hashCode() {
    return delegate.hashCode();
  }

  @Override
  public V value() {
    return delegate.value();
  }

  @Override
  public long creationTime(TimeUnit unit) {
    return delegate.creationTime(unit);
  }

  @Override
  public float hitRate(TimeUnit unit) {
    return delegate.hitRate(unit);
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    return delegate.lastAccessTime(unit);
  }

  public void setAccessTimeMillis(long accessTime) {
    delegate.setAccessTimeMillis(accessTime);
  }

  @Override
  public void setExpireTimeMillis(long expireTime) {
    if (expireTime <= 0 && expireTime != NO_EXPIRE) {
      throw new IllegalArgumentException("invalid expire time: " + expireTime);
    }
    
    this.expireTime = expireTime;
  }
  
  @Override
  public boolean isExpired(long now) {
    final long expire = expireTime;
    if (expire == NO_EXPIRE) {
      return false;
    }
    
    if (expire <= now) {
      return true;
    }
    
    return false;
  }

}
