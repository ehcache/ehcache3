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

class TimeStampedOnHeapByRefValueHolder<V> extends BaseOnHeapByRefValueHolder<V> {

  static final long NO_EXPIRE = -1;

  private final long createTime;

  private volatile long expireTime;
  private volatile long accessTime;

  TimeStampedOnHeapByRefValueHolder(V value, long createTime, long expireTime) {
    super(value);

    setExpireTimeMillis(expireTime);

    this.expireTime = expireTime;
    this.createTime = this.accessTime = createTime;
  }

  @Override
  public long creationTime(TimeUnit unit) {
    return TimeUnit.MILLISECONDS.convert(createTime, unit);
  }

  @Override
  public float hitRate(TimeUnit unit) {
    throw new UnsupportedOperationException("implement me!");
  }

  @Override
  public long lastAccessTime(TimeUnit unit) {
    return TimeUnit.MILLISECONDS.convert(accessTime, unit);
  }

  public void setAccessTimeMillis(long accessTime) {
    this.accessTime = accessTime;
  }

  @Override
  public void setExpireTimeMillis(long expireTime) {
    if (expireTime <= 0 && expireTime != NO_EXPIRE) {
      throw new IllegalArgumentException("invalied expire time: " + expireTime);
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
