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

import org.ehcache.expiry.ExpiryPolicy;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * Eh107Expiry
 */
abstract class Eh107Expiry<K, V> implements ExpiryPolicy<K, V> {
  private final ThreadLocal<Object> shortCircuitAccess = new ThreadLocal<>();

  void enableShortCircuitAccessCalls() {
    shortCircuitAccess.set(this);
  }

  void disableShortCircuitAccessCalls() {
    shortCircuitAccess.remove();
  }

  private boolean isShortCircuitAccessCalls() {
    return shortCircuitAccess.get() != null;
  }

  @Override
  public final Duration getExpiryForAccess(K key, Supplier<? extends V> value) {
    if (isShortCircuitAccessCalls()) {
      return null;
    } else {
      return getExpiryForAccessInternal(key, value);
    }
  }

  protected abstract Duration getExpiryForAccessInternal(K key, Supplier<? extends V> value);
}
