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

package org.ehcache.core.spi.store;

import org.ehcache.core.config.ExpiryUtils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import static java.lang.String.format;
import static org.ehcache.core.config.ExpiryUtils.isExpiryDurationInfinite;

/**
 * @author Ludovic Orban
 */
public abstract class AbstractValueHolder<V> implements Store.ValueHolder<V> {

  private final long id;
  private final long creationTime;
  @SuppressWarnings("CanBeFinal")
  private volatile long lastAccessTime;
  @SuppressWarnings("CanBeFinal")
  private volatile long expirationTime;

  @SuppressWarnings("rawtypes")
  private static final AtomicLongFieldUpdater<AbstractValueHolder> ACCESSTIME_UPDATER = AtomicLongFieldUpdater.newUpdater(AbstractValueHolder.class, "lastAccessTime");
  @SuppressWarnings("rawtypes")
  private static final AtomicLongFieldUpdater<AbstractValueHolder> EXPIRATIONTIME_UPDATER = AtomicLongFieldUpdater.newUpdater(AbstractValueHolder.class, "expirationTime");

  protected AbstractValueHolder(long id, long creationTime) {
    this(id, creationTime, NO_EXPIRE);
  }

  protected AbstractValueHolder(long id, long creationTime, long expirationTime) {
    this.id = id;
    this.creationTime = creationTime;
    this.expirationTime = expirationTime;
    this.lastAccessTime = creationTime;
  }

  @Override
  public long creationTime() {
    return creationTime;
  }

  public void setExpirationTime(long expirationTime, TimeUnit unit) {
    if (expirationTime == NO_EXPIRE) {
      updateExpirationTime(NO_EXPIRE);
    } else if (expirationTime < 0) {
      throw new IllegalArgumentException("invalid expiration time: " + expirationTime);
    } else {
      updateExpirationTime(TimeUnit.MILLISECONDS.convert(expirationTime, unit));
    }
  }

  private void updateExpirationTime(long update) {
    while (true) {
      long current = this.expirationTime;
      if (current >= update) {
        break;
      }
      if (EXPIRATIONTIME_UPDATER.compareAndSet(this, current, update)) {
        break;
      }
    }
  }

  public void accessed(long now, Duration expiration) {
    final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
    if (expiration != null) {
      if (isExpiryDurationInfinite(expiration)) {
        setExpirationTime(Store.ValueHolder.NO_EXPIRE, null);
      } else {
        long newExpirationTime = ExpiryUtils.getExpirationMillis(now, expiration);
        setExpirationTime(newExpirationTime, timeUnit);
      }
    }
    setLastAccessTime(now, timeUnit);
  }

  @Override
  public long expirationTime() {
    long expire = this.expirationTime;
    if (expire == NO_EXPIRE) {
      return NO_EXPIRE;
    }
    return expire;
  }

  @Override
  public boolean isExpired(long expirationTime) {
    long expire = this.expirationTime;
    if (expire == NO_EXPIRE) {
      return false;
    }
    return expire <= expirationTime;
  }

  @Override
  public long lastAccessTime() {
    return lastAccessTime;
  }

  public void setLastAccessTime(long lastAccessTime, TimeUnit unit) {
    long update = unit.convert(lastAccessTime, TimeUnit.MILLISECONDS);
    while (true) {
      long current = this.lastAccessTime;
      if (current >= update) {
        break;
      }
      if (ACCESSTIME_UPDATER.compareAndSet(this, current, update)) {
        break;
      }
    }
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
          other.creationTime == creationTime &&
          other.expirationTime == expirationTime &&
          other.lastAccessTime == lastAccessTime;
    }
    return false;
  }

  @Override
  public long getId() {
    return id;
  }

  @Override
  public String toString() {
    return format("%s", get());
  }
}
