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

import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;

/**
 * EhcacheExpiryWrapper
 */
class EhcacheExpiryWrapper<K, V> extends Eh107Expiry<K, V> {

  private final Expiry<? super K, ? super V> wrappedExpiry;

  EhcacheExpiryWrapper(Expiry<? super K, ? super V> wrappedExpiry) {
    this.wrappedExpiry = wrappedExpiry;
  }

  @Override
  public Duration getExpiryForCreation(K key, V value) {
    return wrappedExpiry.getExpiryForCreation(key, value);
  }

  @Override
  public Duration getExpiryForAccess(K key, V value) {
    return wrappedExpiry.getExpiryForAccess(key, value);
  }

  @Override
  public Duration getExpiryForUpdate(K key, V oldValue, V newValue) {
    return wrappedExpiry.getExpiryForUpdate(key, oldValue, newValue);
  }
}
