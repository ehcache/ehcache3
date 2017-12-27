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

package org.ehcache.core.config;

import org.ehcache.expiry.ExpiryPolicy;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalUnit;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * ExpiryUtils
 */
@SuppressWarnings("deprecation")
public class ExpiryUtils {

  public static boolean isExpiryDurationInfinite(Duration duration) {
    return duration.compareTo(ExpiryPolicy.INFINITE) >= 0;
  }

  public static <K, V> org.ehcache.expiry.Expiry<K, V> convertToExpiry(ExpiryPolicy<K, V> expiryPolicy) {

    if (expiryPolicy == ExpiryPolicy.NO_EXPIRY) {
      @SuppressWarnings("unchecked")
      org.ehcache.expiry.Expiry<K, V> expiry = (org.ehcache.expiry.Expiry<K, V>) org.ehcache.expiry.Expirations.noExpiration();
      return expiry;
    }

    return new org.ehcache.expiry.Expiry<K, V>() {

      @Override
      public org.ehcache.expiry.Duration getExpiryForCreation(K key, V value) {
        return convertDuration(expiryPolicy.getExpiryForCreation(key, value));
      }

      @Override
      public org.ehcache.expiry.Duration getExpiryForAccess(K key, org.ehcache.ValueSupplier<? extends V> value) {
        return convertDuration(expiryPolicy.getExpiryForAccess(key, () -> value.value()));
      }

      @Override
      public org.ehcache.expiry.Duration getExpiryForUpdate(K key, org.ehcache.ValueSupplier<? extends V> oldValue, V newValue) {
        return convertDuration(expiryPolicy.getExpiryForUpdate(key, () -> oldValue.value(), newValue));
      }

      @Override
      public String toString() {
        return "Expiry wrapper of {" +  expiryPolicy + " }";
      }
    };
  }

  private static org.ehcache.expiry.Duration convertDuration(Duration duration) {
    if (duration == null) {
      return null;
    }
    if (duration.isNegative()) {
      throw new IllegalArgumentException("Ehcache duration cannot be negative and so does not accept negative java.time.Duration: " + duration);
    }
    if (duration.isZero()) {
      return org.ehcache.expiry.Duration.ZERO;
    } else {
      long nanos = duration.getNano();
      if (nanos == 0) {
        return org.ehcache.expiry.Duration.of(duration.getSeconds(), TimeUnit.SECONDS);
      }
      long seconds = duration.getSeconds();
      long secondsInNanos = TimeUnit.SECONDS.toNanos(seconds);
      if (secondsInNanos != Long.MAX_VALUE && Long.MAX_VALUE - secondsInNanos > nanos) {
        return org.ehcache.expiry.Duration.of(duration.toNanos(), TimeUnit.NANOSECONDS);
      } else {
        long secondsInMicros = TimeUnit.SECONDS.toMicros(seconds);
        if (secondsInMicros != Long.MAX_VALUE && Long.MAX_VALUE - secondsInMicros > nanos / 1_000) {
          return org.ehcache.expiry.Duration.of(secondsInMicros + nanos / 1_000, TimeUnit.MICROSECONDS);
        } else {
          long secondsInMillis = TimeUnit.SECONDS.toMillis(seconds);
          if (secondsInMillis != Long.MAX_VALUE && Long.MAX_VALUE - secondsInMillis > nanos / 1_000_000) {
            return org.ehcache.expiry.Duration.of(duration.toMillis(), TimeUnit.MILLISECONDS);
          }
        }
      }
      return org.ehcache.expiry.Duration.of(seconds, TimeUnit.SECONDS);
    }
  }

  public static <K, V> ExpiryPolicy<K, V> convertToExpiryPolicy(org.ehcache.expiry.Expiry<K, V> expiry) {
    if (expiry == org.ehcache.expiry.Expirations.noExpiration()) {
      @SuppressWarnings("unchecked")
      ExpiryPolicy<K, V> expiryPolicy = (ExpiryPolicy<K, V>) ExpiryPolicy.NO_EXPIRY;
      return expiryPolicy;
    }

    return new ExpiryPolicy<K, V>() {
      @Override
      public Duration getExpiryForCreation(K key, V value) {
        org.ehcache.expiry.Duration duration = expiry.getExpiryForCreation(key, value);
        return convertDuration(duration);
      }

      @Override
      public Duration getExpiryForAccess(K key, Supplier<? extends V> value) {
        org.ehcache.expiry.Duration duration = expiry.getExpiryForAccess(key, () -> value.get());
        return convertDuration(duration);
      }

      @Override
      public Duration getExpiryForUpdate(K key, Supplier<? extends V> oldValue, V newValue) {
        org.ehcache.expiry.Duration duration = expiry.getExpiryForUpdate(key, () -> oldValue.get(), newValue);
        return convertDuration(duration);
      }

      @Override
      public String toString() {
        return "Expiry wrapper of {" +  expiry + " }";
      }

      private Duration convertDuration(org.ehcache.expiry.Duration duration) {
        if (duration == null) {
          return null;
        }
        if (duration.isInfinite()) {
          return ExpiryPolicy.INFINITE;
        }
        try {
          return Duration.of(duration.getLength(), jucTimeUnitToTemporalUnit(duration.getTimeUnit()));
        } catch (ArithmeticException e) {
          return ExpiryPolicy.INFINITE;
        }
      }
    };
  }

  public static TemporalUnit jucTimeUnitToTemporalUnit(TimeUnit timeUnit) {
    switch (timeUnit) {
      case NANOSECONDS:
        return ChronoUnit.NANOS;
      case MICROSECONDS:
        return ChronoUnit.MICROS;
      case MILLISECONDS:
        return ChronoUnit.MILLIS;
      case SECONDS:
        return ChronoUnit.SECONDS;
      case MINUTES:
        return ChronoUnit.MINUTES;
      case HOURS:
        return ChronoUnit.HOURS;
      case DAYS:
        return ChronoUnit.DAYS;
      default:
        throw new AssertionError("Unkown TimeUnit: " + timeUnit);
    }
  }

  public static long getExpirationMillis(long now, Duration duration) {
    try {
      return duration.plusMillis(now).toMillis();
    } catch (ArithmeticException e) {
      return Long.MAX_VALUE;
    }
  }
}
