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

import org.ehcache.ValueSupplier;

import java.io.Closeable;
import java.io.IOException;

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

class ExpiryPolicyToEhcacheExpiry<K, V> extends Eh107Expiry<K, V> implements Closeable {

  private final ExpiryPolicy expiryPolicy;

  ExpiryPolicyToEhcacheExpiry(ExpiryPolicy expiryPolicy) {
    this.expiryPolicy = expiryPolicy;
  }

  @Override
  public org.ehcache.expiry.Duration getExpiryForCreation(K key, V value) {
    try {
      Duration duration = expiryPolicy.getExpiryForCreation();
      if (duration.isEternal()) {
        return org.ehcache.expiry.Duration.INFINITE;
      }
      return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
    } catch (Throwable t) {
      return org.ehcache.expiry.Duration.ZERO;
    }
  }

  @Override
  public org.ehcache.expiry.Duration getExpiryForAccess(K key, ValueSupplier<? extends V> value) {
    if (isShortCircuitAccessCalls()) {
      return null;
    }

    try {
      Duration duration = expiryPolicy.getExpiryForAccess();
      if (duration == null) {
        return null;
      }
      if (duration.isEternal()) {
        return org.ehcache.expiry.Duration.INFINITE;
      }
      return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
    } catch (Throwable t) {
      return org.ehcache.expiry.Duration.ZERO;
    }
  }

  @Override
  public org.ehcache.expiry.Duration getExpiryForUpdate(K key, ValueSupplier<? extends V> oldValue, V newValue) {
    try {
      Duration duration = expiryPolicy.getExpiryForUpdate();
      if (duration == null) {
        return null;
      }
      if (duration.isEternal()) {
        return org.ehcache.expiry.Duration.INFINITE;
      }
      return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
    } catch (Throwable t) {
      return org.ehcache.expiry.Duration.ZERO;
    }
  }

  @Override
  public void close() throws IOException {
    if (expiryPolicy instanceof Closeable) {
      ((Closeable)expiryPolicy).close();
    }
  }
}
