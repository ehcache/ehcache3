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

import org.ehcache.ValueSupplier;

/**
 * Utility class for getting predefined {@link Expiry} instances.
 */
public final class Expirations {

  /**
   * Get an {@link Expiry} instance for a non expiring (ie. "eternal") cache
   *
   * @return no expiry instance
   */
  public static Expiry<Object, Object> noExpiration() {
    return NoExpiry.INSTANCE;
  }

  /**
   * Get a time-to-live (TTL) {@link Expiry} instance for the given duration
   *
   * @param timeToLive the duration of TTL
   * @param <K> the type of the keys used to access data within the cache
   * @param <V> the type of the values held within the cache
   * @return a TTL expiry
   *
   */
  public static <K, V> Expiry<K, V> timeToLiveExpiration(Duration timeToLive) {
    if (timeToLive == null) {
      throw new NullPointerException("null duration");
    }
    return new TimeToLiveExpiry<K, V>(timeToLive);
  }

  /**
   * Get a time-to-idle (TTI) {@link Expiry} instance for the given duration
   *
   * @param timeToIdle the duration of TTI
   * @param <K> the type of the keys used to access data within the cache
   * @param <V> the type of the values held within the cache
   * @return a TTI expiry
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
    private final Duration update;

    BaseExpiry(Duration create, Duration access, Duration update) {
      this.create = create;
      this.access = access;
      this.update = update;
    }

    @Override
    public Duration getExpiryForCreation(K key, V value) {
      return create;
    }

    @Override
    public Duration getExpiryForAccess(K key, ValueSupplier<? extends V> value) {
      return access;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final BaseExpiry<?, ?> that = (BaseExpiry<?, ?>)o;

      if (access != null ? !access.equals(that.access) : that.access != null) return false;
      if (create != null ? !create.equals(that.create) : that.create != null) return false;
      if (update != null ? !update.equals(that.update) : that.update != null) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = create != null ? create.hashCode() : 0;
      result = 31 * result + (access != null ? access.hashCode() : 0);
      result = 31 * result + (update != null ? update.hashCode() : 0);
      return result;
    }

    @Override
    public String toString() {
      return this.getClass().getSimpleName() + "{" +
          "create=" + create +
          ", access=" + access +
          ", update=" + update +
          '}';
    }

    @Override
    public Duration getExpiryForUpdate(K key, ValueSupplier<? extends V> oldValue, V newValue) {
      return update;
    }
  }

  private static class TimeToLiveExpiry<K, V> extends BaseExpiry<K, V> {
    TimeToLiveExpiry(Duration ttl) {
      super(ttl, null, ttl);
    }
  }

  private static class TimeToIdleExpiry<K, V> extends BaseExpiry<K, V> {
    TimeToIdleExpiry(Duration tti) {
      super(tti, tti, tti);
    }
  }

  private static class NoExpiry<K, V> extends BaseExpiry<K, V> {

    private static final Expiry<Object, Object> INSTANCE = new NoExpiry<Object, Object>();

    private NoExpiry() {
      super(Duration.FOREVER, null, null);
    }
  }
}
