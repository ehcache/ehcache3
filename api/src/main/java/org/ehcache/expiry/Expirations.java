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
package org.ehcache.expiry;

public class Expirations {

  /**
   * Get an {@link Expiry} instance for a non expiring (ie. "eternal") cache
   */
  @SuppressWarnings("unchecked")
  public static <K, V> Expiry<K, V> noExpiration() {
    return (Expiry<K, V>) NoExpiry.INSTANCE;
  }

  /**
   * Get a time-to-live (TTL) {@link Expiry} instance for the given duration
   */
  public static <K, V> Expiry<K, V> timeToLiveExpiration(Duration timeToLive) {
    if (timeToLive == null) {
      throw new NullPointerException("null duration");
    }
    return new TimeToLiveExpiry<K, V>(timeToLive);
  }

  /**
   * Get a time-to-idle (TTI) {@link Expiry} instance for the given duration
   */
  public static <K, V> Expiry<K, V> timeToIdleExpiration(Duration timeToIdle) {
    if (timeToIdle == null) {
      throw new NullPointerException("null duration");
    }
    return new TimeToIdleExpiry<K, V>(timeToIdle);
  }

  private Expirations() {
    //
  }

  private static abstract class BaseExpiry<K, V> implements Expiry<K, V> {

    private final Duration create;
    private final Duration access;
    
    BaseExpiry(Duration create, Duration access) {
      this.create = create;
      this.access = access;
    }

    @Override
    public Duration getExpiryForCreation(K key, V value) {
      return create;
    }
    
    @Override
    public Duration getExpiryForAccess(K key, V value) {
      return access;
    }
  }

  private static class TimeToLiveExpiry<K, V> extends BaseExpiry<K, V> {
    TimeToLiveExpiry(Duration ttl) {
      super(ttl, null);
    }
  }

  private static class TimeToIdleExpiry<K, V> extends BaseExpiry<K, V> {
    TimeToIdleExpiry(Duration tti) {
      super(tti, tti);
    }
  }

  private static class NoExpiry<K, V> extends BaseExpiry<K, V> {

    private static final Expiry<?, ?> INSTANCE = new NoExpiry<Object, Object>();

    private NoExpiry() {
      super(Duration.FOREVER, null);
    }
  }
}
