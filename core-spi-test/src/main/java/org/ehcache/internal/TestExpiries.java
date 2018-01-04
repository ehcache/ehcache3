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

package org.ehcache.internal;

import org.ehcache.expiry.ExpiryPolicy;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * TestExpiries
 */
public class TestExpiries {

  public static <K, V> ExpiryPolicy<K, V> tTI(Duration duration) {
    return new ExpiryPolicy<K, V>() {
      @Override
      public Duration getExpiryForCreation(K key, V value) {
        return duration;
      }

      @Override
      public Duration getExpiryForAccess(K key, Supplier<? extends V> value) {
        return duration;
      }

      @Override
      public Duration getExpiryForUpdate(K key, Supplier<? extends V> oldValue, V newValue) {
        return duration;
      }
    };
  }

  public static <K, V> ExpiryPolicy<K, V> tTL(Duration duration) {
    return new ExpiryPolicy<K, V>() {
      @Override
      public Duration getExpiryForCreation(K key, V value) {
        return duration;
      }

      @Override
      public Duration getExpiryForAccess(K key, Supplier<? extends V> value) {
        return null;
      }

      @Override
      public Duration getExpiryForUpdate(K key, Supplier<? extends V> oldValue, V newValue) {
        return duration;
      }
    };
  }

  public static <K, V> ExpiryPolicy<K, V> custom(Duration creation, Duration access, Duration update) {
    return new ExpiryPolicy<K, V>() {
      @Override
      public Duration getExpiryForCreation(K key, V value) {
        return creation;
      }

      @Override
      public Duration getExpiryForAccess(K key, Supplier<? extends V> value) {
        return access;
      }

      @Override
      public Duration getExpiryForUpdate(K key, Supplier<? extends V> oldValue, V newValue) {
        return update;
      }
    };
  }
}
