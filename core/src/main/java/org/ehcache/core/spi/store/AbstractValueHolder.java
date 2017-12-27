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

  private static final AtomicLongFieldUpdater<AbstractValueHolder> HITS_UPDATER = AtomicLongFieldUpdater.newUpdater(AbstractValueHolder.class, "hits");
  private final long id;
  private final long creationTime;
  @SuppressWarnings("CanBeFinal")
  private volatile long lastAccessTime;
  @SuppressWarnings("CanBeFinal")
  private volatile long expirationTime;
  private volatile long hits;

  private static final AtomicLongFieldUpdater<AbstractValueHolder> ACCESSTIME_UPDATER = AtomicLongFieldUpdater.newUpdater(AbstractValueHolder.class, "lastAccessTime");
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

  protected abstract TimeUnit nativeTimeUnit();

  @Override
  public long creationTime(TimeUnit unit) {
    return unit.convert(creationTime, nativeTimeUnit());
  }

  public void setExpirationTime(long expirationTime, TimeUnit unit) {
    if (expirationTime == NO_EXPIRE) {
      updateExpirationTime(NO_EXPIRE);
    } else if (expirationTime <= 0) {
      throw new IllegalArgumentException("invalid expiration time: " + expirationTime);
    } else {
      updateExpirationTime(nativeTimeUnit().convert(expirationTime, unit));
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
    final TimeUnit timeUnit = nativeTimeUnit();
    if (expiration != null) {
      if (isExpiryDurationInfinite(expiration)) {
        setExpirationTime(Store.ValueHolder.NO_EXPIRE, null);
      } else {
        long newExpirationTime = ExpiryUtils.getExpirationMillis(now, expiration);
        setExpirationTime(newExpirationTime, timeUnit);
      }
    }
    setLastAccessTime(now, timeUnit);
    HITS_UPDATER.getAndIncrement(this);
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
    long update = unit.convert(lastAccessTime, nativeTimeUnit());
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
          other.creationTime(nativeTimeUnit()) == creationTime && creationTime(other.nativeTimeUnit()) == other.creationTime &&
          other.expirationTime(nativeTimeUnit()) == expirationTime && expirationTime(other.nativeTimeUnit()) == other.expirationTime &&
          other.lastAccessTime(nativeTimeUnit()) == lastAccessTime && lastAccessTime(other.nativeTimeUnit()) == other.lastAccessTime;
    }
    return false;
  }

  @Override
  public float hitRate(long now, TimeUnit unit) {
    final long endTime = TimeUnit.NANOSECONDS.convert(now, TimeUnit.MILLISECONDS);
    final long startTime = TimeUnit.NANOSECONDS.convert(creationTime, nativeTimeUnit());
    float duration = (endTime - startTime)/(float)TimeUnit.NANOSECONDS.convert(1, unit);
    return (hits/duration);
  }

  @Override
  public long hits() {
    return this.hits;
  }

  protected void setHits(long hits) {
    HITS_UPDATER.set(this, hits);
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
