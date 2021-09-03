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
package org.ehcache.jsr107;

import org.ehcache.core.config.ExpiryUtils;

import java.io.Closeable;
import java.io.IOException;
import java.util.function.Supplier;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

class ExpiryPolicyToEhcacheExpiry<K, V> extends Eh107Expiry<K, V> implements Closeable {

  private final ExpiryPolicy expiryPolicy;

  ExpiryPolicyToEhcacheExpiry(ExpiryPolicy expiryPolicy) {
    this.expiryPolicy = expiryPolicy;
  }

  @Override
  public java.time.Duration getExpiryForCreation(K key, V value) {
    try {
      Duration duration = expiryPolicy.getExpiryForCreation();
      return convertDuration(duration);
    } catch (Throwable t) {
      return java.time.Duration.ZERO;
    }
  }

  @Override
  protected java.time.Duration getExpiryForAccessInternal(K key, Supplier<? extends V> value) {
    try {
      Duration duration = expiryPolicy.getExpiryForAccess();
      if (duration == null) {
        return null;
      }
      return convertDuration(duration);
    } catch (Throwable t) {
      return java.time.Duration.ZERO;
    }
  }

  @Override
  public java.time.Duration getExpiryForUpdate(K key, Supplier<? extends V> oldValue, V newValue) {
    try {
      Duration duration = expiryPolicy.getExpiryForUpdate();
      if (duration == null) {
        return null;
      }
      return convertDuration(duration);
    } catch (Throwable t) {
      return java.time.Duration.ZERO;
    }
  }

  @Override
  public void close() throws IOException {
    if (expiryPolicy instanceof Closeable) {
      ((Closeable)expiryPolicy).close();
    }
  }

  private java.time.Duration convertDuration(Duration duration) {
    if (duration.isEternal()) {
      return org.ehcache.expiry.ExpiryPolicy.INFINITE;
    }
    return java.time.Duration.of(duration.getDurationAmount(), ExpiryUtils.jucTimeUnitToTemporalUnit(duration.getTimeUnit()));
  }
}
