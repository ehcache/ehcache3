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
   * Get an {@link Expiry} instance for a non expiring (ie. "eternal") cache.
   *
   * @return the no expiry instance
   */
  public static Expiry<Object, Object> noExpiration() {
    return NoExpiry.INSTANCE;
  }

  /**
   * Get a time-to-live (TTL) {@link Expiry} instance for the given {@link Duration}.
   *
   * @param timeToLive the TTL duration
   * @return a TTL expiry
   */
  public static Expiry<Object, Object> timeToLiveExpiration(Duration timeToLive) {
    if (timeToLive == null) {
      throw new NullPointerException("Duration cannot be null");
    }
    return new TimeToLiveExpiry(timeToLive);
  }

  /**
   * Get a time-to-idle (TTI) {@link Expiry} instance for the given {@link Duration}.
   *
   * @param timeToIdle the TTI duration
   * @return a TTI expiry
   */
  public static Expiry<Object, Object> timeToIdleExpiration(Duration timeToIdle) {
    if (timeToIdle == null) {
      throw new NullPointerException("Duration cannot be null");
    }
    return new TimeToIdleExpiry(timeToIdle);
  }

  /**
   * Fluent API for creating an Expiry instance where you can specify constant values for creation, access and update time.
   * Unspecified values will be set to {@code Duration.INFINITE} for create and {@code null} for access and update, matching
   * the {@link #noExpiration() no expiration} expiry.
   *
   * @param <K> the key type for the cache
   * @param <V> the value type for the cache
   * @return an {@link Expiry} builder
   */
  public static <K, V> ExpiryBuilder<K, V> builder() {
    return new ExpiryBuilder<K, V>();
  }

  private Expirations() {
    //
  }

  /**
   * Simple implementation of the {@link Expiry} interface allowing to set constants to each expiry types.
   */
  private static class BaseExpiry<K, V> implements Expiry<K, V> {

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
    public Duration getExpiryForUpdate(K key, ValueSupplier<? extends V> oldValue, V newValue) {
      return update;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final BaseExpiry that = (BaseExpiry)o;

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
  }

  private static class TimeToLiveExpiry extends BaseExpiry<Object, Object> {
    TimeToLiveExpiry(Duration ttl) {
      super(ttl, null, ttl);
    }
  }

  private static class TimeToIdleExpiry extends BaseExpiry<Object, Object> {
    TimeToIdleExpiry(Duration tti) {
      super(tti, tti, tti);
    }
  }

  private static class NoExpiry extends BaseExpiry<Object, Object> {

    private static final Expiry<Object, Object> INSTANCE = new NoExpiry();

    private NoExpiry() {
      super(Duration.INFINITE, null, null);
    }
  }

  /**
   * Builder to create a simple {@link Expiry}.
   *
   * @param <K> Key type of the cache entries
   * @param <V> Value type of the cache entries
   */
  public static final class ExpiryBuilder<K, V> {

    private Duration create = Duration.INFINITE;
    private Duration access = null;
    private Duration update = null;

    private ExpiryBuilder() {}

    /**
     * Set TTL since creation
     *
     * @param create TTL since creation
     * @return this builder
     */
    public ExpiryBuilder<K, V> setCreate(Duration create) {
      this.create = create;
      return this;
    }

    /**
     * Set TTL since last access
     *
     * @param access TTL since last access
     * @return this builder
     */
    public ExpiryBuilder<K, V> setAccess(Duration access) {
      this.access = access;
      return this;
    }

    /**
     * Set TTL since last update
     *
     * @param update TTL since last update
     * @return this builder
     */
    public ExpiryBuilder<K, V> setUpdate(Duration update) {
      this.update = update;
      return this;
    }

    /**
     *
     * @return an {@link Expiry}
     */
    public Expiry<K, V> build() {
      return new BaseExpiry<K, V>(create, access, update);
    }
  }
}
