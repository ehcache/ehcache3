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

import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;

class Eh107Expiry implements org.ehcache.expiry.Expiry<Object, Object> {

  private final ThreadLocal<Object> shortCircuitAccess = new ThreadLocal<Object>();
  private final ExpiryPolicy expiryPolicy;

  Eh107Expiry(ExpiryPolicy expiryPolicy) {
    this.expiryPolicy = expiryPolicy;
  }

  @Override
  public org.ehcache.expiry.Duration getExpiryForCreation(Object key, Object value) {
    try {
      Duration duration = expiryPolicy.getExpiryForCreation();
      if (duration.isEternal()) {
        return org.ehcache.expiry.Duration.FOREVER;
      }
      return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
    } catch (Throwable t) {
      return org.ehcache.expiry.Duration.ZERO;
    }
  }

  @Override
  public org.ehcache.expiry.Duration getExpiryForAccess(Object key, Object value) {
    if (shortCircuitAccess.get() != null) {
      return null;
    }

    try {
      Duration duration = expiryPolicy.getExpiryForAccess();
      if (duration == null) {
        return null;
      }
      if (duration.isEternal()) {
        return org.ehcache.expiry.Duration.FOREVER;
      }
      return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
    } catch (Throwable t) {
      return org.ehcache.expiry.Duration.ZERO;
    }
  }

  @Override
  public org.ehcache.expiry.Duration getExpiryForUpdate(Object key, Object oldValue, Object newValue) {
    try {
      Duration duration = expiryPolicy.getExpiryForUpdate();
      if (duration == null) {
        return null;
      }
      if (duration.isEternal()) {
        return org.ehcache.expiry.Duration.FOREVER;
      }
      return new org.ehcache.expiry.Duration(duration.getDurationAmount(), duration.getTimeUnit());
    } catch (Throwable t) {
      return org.ehcache.expiry.Duration.ZERO;
    }
  }

  void enableShortCircuitAccessCalls() {
    shortCircuitAccess.set(this);
  }

  void disableShortCircuitAccessCalls() {
    shortCircuitAccess.remove();
  }
}
