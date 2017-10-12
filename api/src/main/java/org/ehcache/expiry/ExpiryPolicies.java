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

import java.util.Objects;

/**
 * Utility class for getting predefined {@link ExpiryPolicy} instances.
 */
public final class ExpiryPolicies {

  /**
   * Get an {@link ExpiryPolicy} instance for a non expiring (ie. "eternal") cache.
   *
   * @return the no expiry instance
   */
  public static ExpiryPolicy<Object, Object> noExpiration() {
    return NoExpiryPolicy.INSTANCE;
  }

  /**
   * Get a time-to-live (TTL) {@link ExpiryPolicy} instance for the given {@link java.time.Duration}.
   *
   * @param timeToLive the TTL duration
   * @return a TTL expiry
   */
  public static ExpiryPolicy<Object, Object> timeToLiveExpiration(java.time.Duration timeToLive) {
    Objects.requireNonNull(timeToLive, "TTL duration cannot be null");
    if (timeToLive.isNegative()) {
      throw new IllegalArgumentException("TTL duration cannot be negative");
    }
    return new TimeToLiveExpiryPolicy(timeToLive);
  }

  /**
   * Get a time-to-idle (TTI) {@link ExpiryPolicy} instance for the given {@link java.time.Duration}.
   *
   * @param timeToIdle the TTI duration
   * @return a TTI expiry
   */
  public static ExpiryPolicy<Object, Object> timeToIdleExpiration(java.time.Duration timeToIdle) {
    Objects.requireNonNull(timeToIdle, "TTI duration cannot be null");
    if (timeToIdle.isNegative()) {
      throw new IllegalArgumentException("TTI duration cannot be negative");
    }
    return new TimeToIdleExpiryPolicy(timeToIdle);
  }

  /**
   * Fluent API for creating an {@link ExpiryPolicy} instance where you can specify constant values for creation, access and update time.
   * Unspecified values will be set to {@link ExpiryPolicy#INFINITE INFINITE} for create and {@code null} for access and update, matching
   * the {@link #noExpiration()}  no expiration} expiry.
   *
   * @param <K> the key type for the cache
   * @param <V> the value type for the cache
   * @return an {@link ExpiryPolicy} builder
   */
  public static <K, V> ExpiryPolicyBuilder<K, V> builder() {
    return new ExpiryPolicyBuilder<>();
  }


  private ExpiryPolicies() {
    //
  }

  /**
   * Simple implementation of the {@link ExpiryPolicy} interface allowing to set constants to each expiry types.
   */
  private static class BaseExpiryPolicy<K, V> implements ExpiryPolicy<K, V> {

    private final java.time.Duration create;
    private final java.time.Duration access;
    private final java.time.Duration update;

    BaseExpiryPolicy(java.time.Duration create, java.time.Duration access, java.time.Duration update) {
      this.create = create;
      this.access = access;
      this.update = update;
    }
    @Override
    public java.time.Duration getExpiryForCreation(K key, V value) {
      return create;
    }

    @Override
    public java.time.Duration getExpiryForAccess(K key, ValueSupplier<? extends V> value) {
      return access;
    }

    @Override
    public java.time.Duration getExpiryForUpdate(K key, ValueSupplier<? extends V> oldValue, V newValue) {
      return update;
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      final BaseExpiryPolicy that = (BaseExpiryPolicy) o;

      if (!Objects.equals(access, that.access)) return false;
      if (!Objects.equals(create, that.create)) return false;
      if (!Objects.equals(update, that.update)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = Objects.hashCode(create);
      result = 31 * result + Objects.hashCode(access);
      result = 31 * result + Objects.hashCode(update);
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

  private static class TimeToLiveExpiryPolicy extends BaseExpiryPolicy<Object, Object> {
    TimeToLiveExpiryPolicy(java.time.Duration ttl) {
      super(ttl, null, ttl);
    }
  }

  private static class TimeToIdleExpiryPolicy extends BaseExpiryPolicy<Object, Object> {
    TimeToIdleExpiryPolicy(java.time.Duration tti) {
      super(tti, tti, tti);
    }
  }

  private static class NoExpiryPolicy extends BaseExpiryPolicy<Object, Object> {

    private static final ExpiryPolicy<Object, Object> INSTANCE = new NoExpiryPolicy();

    private NoExpiryPolicy() {
      super(ExpiryPolicy.INFINITE, null, null);
    }
  }

  /**
   * Builder to create a simple {@link Expiry}.
   *
   * @param <K> Key type of the cache entries
   * @param <V> Value type of the cache entries
   */
  public static final class ExpiryPolicyBuilder<K, V> {

    private java.time.Duration create = ExpiryPolicy.INFINITE;
    private java.time.Duration access = null;
    private java.time.Duration update = null;

    private ExpiryPolicyBuilder() {}

    /**
     * Set TTL since creation
     *
     * @param create TTL since creation
     * @return this builder
     */
    public ExpiryPolicyBuilder<K, V> setCreate(java.time.Duration create) {
      Objects.requireNonNull(create, "Create duration cannot be null");
      if (create.isNegative()) {
        throw new IllegalArgumentException("Create duration must be positive");
      }
      this.create = create;
      return this;
    }

    /**
     * Set TTL since last access
     *
     * @param access TTL since last access
     * @return this builder
     */
    public ExpiryPolicyBuilder<K, V> setAccess(java.time.Duration access) {
      if (access != null && access.isNegative()) {
        throw new IllegalArgumentException("Access duration must be positive");
      }
      this.access = access;
      return this;
    }

    /**
     * Set TTL since last update
     *
     * @param update TTL since last update
     * @return this builder
     */
    public ExpiryPolicyBuilder<K, V> setUpdate(java.time.Duration update) {
      if (update != null && update.isNegative()) {
        throw new IllegalArgumentException("Update duration must be positive");
      }
      this.update = update;
      return this;
    }

    /**
     *
     * @return an {@link Expiry}
     */
    public ExpiryPolicy<K, V> build() {
      return new BaseExpiryPolicy<>(create, access, update);
    }
  }
}
