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

package org.ehcache.impl.internal.store.heap;

import org.ehcache.core.events.StoreEventSink;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Specialized behavior for an OnHeapStore allowing optimization depending on the expiry policy used.
 *
 * @param <K> type of the keys stored
 * @param <V> type of the values stored
 */
interface OnHeapStrategy<K, V> {

  Logger LOG = LoggerFactory.getLogger(OnHeapStore.class);

  static <K, V> OnHeapStrategy<K, V> strategy(OnHeapStore<K, V> store, ExpiryPolicy<? super K, ? super V> expiry, TimeSource timeSource) {
    if(expiry == ExpiryPolicy.NO_EXPIRY) {
      LOG.debug("No expiration strategy detected");
      return new NoExpirationStrategy<>();
    }
    if(expiry.getClass().getName().equals("org.ehcache.config.builders.ExpiryPolicyBuilder$TimeToLiveExpiryPolicy")) {
      LOG.debug("TTL expiration strategy detected");
      return new TTLStrategy<>(expiry, timeSource);
    }
    LOG.debug("TTI or custom expiration strategy detected");
    return new AllStrategy<>(store, expiry, timeSource);
  }

  /**
   * Tells if a given mapping is expired.
   *
   * @param mapping mapping to test for expiration
   * @return if the mapping is expired
   */
  boolean isExpired(OnHeapValueHolder<V> mapping);

  /**
   * Set the access time on the mapping and its expiry time if it is access sensitive (TTI). We  expect this action to
   * be called when the caller isn't holding any lock.
   *
   * @param key key of the mapping. Used to remove it form the map if needed
   * @param valueHolder the mapping
   * @param now the current time
   */
  void setAccessAndExpiryTimeWhenCallerOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now);

  /**
   * Set the access time on the mapping and its expiry time if it is access sensitive (TTI). We  expect this action to
   * be called when the caller is currently holding a lock.
   *
   * @param key key of the mapping. Used to remove it form the map if needed
   * @param valueHolder the mapping
   * @param now the current time
   * @param eventSink sink where the expiration request will be sent
   * @return the mapping or null if it was removed
   */
  OnHeapValueHolder<V> setAccessAndExpiryWhenCallerlUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now, StoreEventSink<K, V> eventSink);

  /**
   * Get the new expiry duration as per {@link ExpiryPolicy#getExpiryForAccess(Object, Supplier)}.
   *
   * @param key key of the mapping
   * @param valueHolder the mapping
   * @return new access expiry duration
   */
  Duration getAccessDuration(K key, Store.ValueHolder<V> valueHolder);

  /**
   * Get the new expiry duration as per {@link ExpiryPolicy#getExpiryForUpdate(Object, Supplier, Object)}.
   *
   * @param key key of the mapping
   * @param oldValue the old mapping to be updated
   * @param newValue the new value for the mapping
   * @return new access expiry duration
   */
  Duration getUpdateDuration(K key, OnHeapValueHolder<V> oldValue, V newValue);

  /**
   * All purpose strategy. Covers any case that can't be optimized due to the uncertainty of the expiry policy used.
   *
   * @param <K> type of the keys stored
   * @param <V> type of the values stored
   */
  class AllStrategy<K, V> implements OnHeapStrategy<K, V> {
    private final OnHeapStore<K, V> store;
    private final ExpiryPolicy<? super K, ? super V> expiry;
    private final TimeSource timeSource;

    public AllStrategy(OnHeapStore<K, V> store, ExpiryPolicy<? super K, ? super V> expiry, TimeSource timeSource) {
      this.store = store;
      this.expiry = expiry;
      this.timeSource = timeSource;
    }

    @Override
    public boolean isExpired(OnHeapValueHolder<V> mapping) {
      return mapping.isExpired(timeSource.getTimeMillis());
    }

    @Override
    public void setAccessAndExpiryTimeWhenCallerOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now) {
      Duration duration = getAccessDuration(key, valueHolder);
      if (Duration.ZERO.equals(duration)) {
        // Expires mapping through computeIfPresent
        store.expireMappingUnderLock(key, valueHolder);
      } else {
        valueHolder.accessed(now, duration);
      }
    }

    public Duration getAccessDuration(K key, Store.ValueHolder<V> valueHolder) {
      Duration duration;
      try {
        duration = expiry.getExpiryForAccess(key, valueHolder);
        if (duration != null && duration.isNegative()) {
          duration = Duration.ZERO;
        }
      } catch (RuntimeException re) {
        LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
        duration = Duration.ZERO;
      }
      return duration;
    }

    public Duration getUpdateDuration(K key, OnHeapValueHolder<V> oldValue, V newValue) {
      Duration duration;
      try {
        duration = expiry.getExpiryForUpdate(key, oldValue, newValue);
        if (duration != null && duration.isNegative()) {
          duration = Duration.ZERO;
        }
      } catch (RuntimeException re) {
        LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
        duration = Duration.ZERO;
      }
      return duration;
    }

    public OnHeapValueHolder<V> setAccessAndExpiryWhenCallerlUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now,
                                                                       StoreEventSink<K, V> eventSink) {
      Duration duration = getAccessDuration(key, valueHolder);
      if (Duration.ZERO.equals(duration)) {
        // Fires event, must happen under lock
        store.fireOnExpirationEvent(key, valueHolder, eventSink);
        return null;
      } else {
        valueHolder.accessed(now, duration);
      }
      return valueHolder;
    }

  }

  /**
   * Strategy used when entries are never expiring.
   *
   * @param <K> type of the keys stored
   * @param <V> type of the values stored
   */
  class NoExpirationStrategy<K, V> implements OnHeapStrategy<K, V> {

    @Override
    public boolean isExpired(OnHeapValueHolder<V> mapping) {
      return false;
    }

    @Override
    public void setAccessAndExpiryTimeWhenCallerOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now) {
      valueHolder.accessed(now, null);
    }

    public OnHeapValueHolder<V> setAccessAndExpiryWhenCallerlUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now,
                                                                       StoreEventSink<K, V> eventSink) {
      valueHolder.accessed(now, null);
      return valueHolder;
    }

    public Duration getAccessDuration(K key, Store.ValueHolder<V> valueHolder) {
      return null;
    }

    public Duration getUpdateDuration(K key, OnHeapValueHolder<V> oldValue, V newValue) {
      return null;
    }
  }

  /**
   * Strategy used when entries are expiring due to TTL only.
   *
   * @param <K> type of the keys stored
   * @param <V> type of the values stored
   */
  class TTLStrategy<K, V> implements OnHeapStrategy<K, V> {
    private final TimeSource timeSource;
    private final ExpiryPolicy<? super K, ? super V> expiry;

    public TTLStrategy(ExpiryPolicy<? super K, ? super V> expiry, TimeSource timeSource) {
      this.timeSource = timeSource;
      this.expiry = expiry;
    }

    @Override
    public boolean isExpired(OnHeapValueHolder<V> mapping) {
      return mapping.isExpired(timeSource.getTimeMillis());
    }

    @Override
    public void setAccessAndExpiryTimeWhenCallerOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now) {
      valueHolder.accessed(now, null);
    }

    public OnHeapValueHolder<V> setAccessAndExpiryWhenCallerlUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now,
                                                                       StoreEventSink<K, V> eventSink) {
      valueHolder.accessed(now, null);
      return valueHolder;
    }

    public Duration getAccessDuration(K key, Store.ValueHolder<V> valueHolder) {
      return null;
    }

    public Duration getUpdateDuration(K key, OnHeapValueHolder<V> oldValue, V newValue) {
      Duration duration;
      try {
        duration = expiry.getExpiryForUpdate(key, oldValue, newValue);
        if (duration != null && duration.isNegative()) {
          duration = Duration.ZERO;
        }
      } catch (RuntimeException re) {
        LOG.error("Expiry computation caused an exception - Expiry duration will be 0 ", re);
        duration = Duration.ZERO;
      }
      return duration;
    }
  }

}
