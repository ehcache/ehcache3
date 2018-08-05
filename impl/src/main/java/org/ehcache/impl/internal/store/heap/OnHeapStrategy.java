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
import org.ehcache.core.spi.time.TimeSource;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.internal.store.heap.holders.OnHeapValueHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * @author Henri Tremblay
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
      return new TTLStrategy<>(timeSource);
    }
    LOG.debug("TTI or custom expiration strategy detected");
    return new AllStrategy<>(store, expiry, timeSource);
  }

  boolean isExpired(OnHeapValueHolder<V> mapping);

  void setAccessTimeAndExpiryThenReturnMappingOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now);

  OnHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMappingUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now, StoreEventSink<K, V> eventSink);

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
      return mapping.isExpired(timeSource.getTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void setAccessTimeAndExpiryThenReturnMappingOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now) {
      Duration duration = getAccessDuration(key, valueHolder);
      if (Duration.ZERO.equals(duration)) {
        // Expires mapping through computeIfPresent
        store.expireMappingUnderLock(key, valueHolder);
      } else {
        valueHolder.accessed(now, duration);
      }
    }

    private Duration getAccessDuration(K key, OnHeapValueHolder<V> valueHolder) {
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

    public OnHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMappingUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now,
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

  class NoExpirationStrategy<K, V> implements OnHeapStrategy<K, V> {

    @Override
    public boolean isExpired(OnHeapValueHolder<V> mapping) {
      return false;
    }

    @Override
    public void setAccessTimeAndExpiryThenReturnMappingOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now) {
      valueHolder.accessed(now, null);
    }

    public OnHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMappingUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now,
                                                                                 StoreEventSink<K, V> eventSink) {
      valueHolder.accessed(now, null);
      return valueHolder;
    }
  }

  class TTLStrategy<K, V> implements OnHeapStrategy<K, V> {
    private final TimeSource timeSource;

    public TTLStrategy(TimeSource timeSource) {
      this.timeSource = timeSource;
    }

    @Override
    public boolean isExpired(OnHeapValueHolder<V> mapping) {
      return mapping.isExpired(timeSource.getTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void setAccessTimeAndExpiryThenReturnMappingOutsideLock(K key, OnHeapValueHolder<V> valueHolder, long now) {
      valueHolder.accessed(now, null);
    }

    public OnHeapValueHolder<V> setAccessTimeAndExpiryThenReturnMappingUnderLock(K key, OnHeapValueHolder<V> valueHolder, long now,
                                                                                 StoreEventSink<K, V> eventSink) {
      valueHolder.accessed(now, null);
      return valueHolder;
    }
  }

}
