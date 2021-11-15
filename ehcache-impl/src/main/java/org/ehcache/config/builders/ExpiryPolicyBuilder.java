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
package org.ehcache.config.builders;

import org.ehcache.config.Builder;
import org.ehcache.expiry.ExpiryPolicy;

import java.time.Duration;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Supplier;

/**
 * Builder and utilities for getting predefined {@link ExpiryPolicy} instances.
 */
public final class ExpiryPolicyBuilder<K, V> implements Builder<ExpiryPolicy<K, V>>{

  /**
   * Get an {@link ExpiryPolicy} instance for a non expiring (ie. "eternal") cache.
   *
   * @return the no expiry instance
   */
  public static ExpiryPolicy<Object, Object> noExpiration() {
    return ExpiryPolicy.NO_EXPIRY;
  }

  /**
   * Get a time-to-live (TTL) {@link ExpiryPolicy} instance for the given {@link Duration}.
   *
   * @param timeToLive the TTL duration
   * @return a TTL expiry
   */
  public static ExpiryPolicy<Object, Object> timeToLiveExpiration(Duration timeToLive) {
    Objects.requireNonNull(timeToLive, "TTL duration cannot be null");
    if (timeToLive.isNegative()) {
      throw new IllegalArgumentException("TTL duration cannot be negative");
    }
    return new TimeToLiveExpiryPolicy(timeToLive);
  }

  /**
   * Get a time-to-idle (TTI) {@link ExpiryPolicy} instance for the given {@link Duration}.
   *
   * @param timeToIdle the TTI duration
   * @return a TTI expiry
   */
  public static ExpiryPolicy<Object, Object> timeToIdleExpiration(Duration timeToIdle) {
    Objects.requireNonNull(timeToIdle, "TTI duration cannot be null");
    if (timeToIdle.isNegative()) {
      throw new IllegalArgumentException("TTI duration cannot be negative");
    }
    return new TimeToIdleExpiryPolicy(timeToIdle);
  }

  @FunctionalInterface
  public interface TriFunction<T, U, V, R> {
    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @param v the third function argument
     * @return the function result
     */
    R apply(T t, U u, V v);
  }

  /**
   * Fluent API for creating an {@link ExpiryPolicy} instance where you can specify constant values for creation, access and update time.
   * Unspecified values will be set to {@link ExpiryPolicy#INFINITE INFINITE} for create and {@code null} for access and update, matching
   * the {@link #noExpiration()}  no expiration} expiry.
   *
   * @return an {@link ExpiryPolicy} builder
   */
  public static ExpiryPolicyBuilder<Object, Object> expiry() {
    return new ExpiryPolicyBuilder<>();
  }

  private final BiFunction<? super K, ? super V, Duration> create;
  private final BiFunction<? super K, ? super Supplier<? extends V>, Duration> access;
  private final TriFunction<? super K, ? super Supplier<? extends V>, ? super V, Duration> update;

  private ExpiryPolicyBuilder() {
    this((k, v) -> ExpiryPolicy.INFINITE, (k, v) -> null, (k, oldV, newV) -> null);
  }

  private ExpiryPolicyBuilder(BiFunction<? super K, ? super V, Duration> create,
                              BiFunction<? super K, ? super Supplier<? extends V>, Duration> access,
                              TriFunction<? super K, ? super Supplier<? extends V>, ? super V, Duration> update) {
    this.create = create;
    this.access = access;
    this.update = update;
  }

  /**
   * Set TTL since creation.
   * <p>
   * Note: Calling this method on a builder with an existing TTL since creation will override the previous value or function.
   *
   * @param create TTL since creation
   * @return a new builder with the TTL since creation
   */
  public ExpiryPolicyBuilder<K, V> create(Duration create) {
    Objects.requireNonNull(create, "Create duration cannot be null");
    if (create.isNegative()) {
      throw new IllegalArgumentException("Create duration must be positive");
    }
    return create((a, b) -> create);
  }

  /**
   * Set a function giving the TTL since creation.
   * <p>
   * Note: Calling this method on a builder with an existing TTL since creation will override the previous value or function.
   *
   * @param create Function giving the TTL since creation
   * @return a new builder with the TTL creation calculation function
   */
  public <K2 extends K, V2 extends V> ExpiryPolicyBuilder<K2, V2> create(BiFunction<K2, V2, Duration> create) {
    return new ExpiryPolicyBuilder<>(Objects.requireNonNull(create), access, update);
  }

  /**
   * Set TTI since last access.
   * <p>
   * Note: Calling this method on a builder with an existing TTI since last access will override the previous value or function.
   *
   * @param access TTI since last access
   * @return a new builder with the TTI since last access
   */
  public ExpiryPolicyBuilder<K, V> access(Duration access) {
    if (access != null && access.isNegative()) {
      throw new IllegalArgumentException("Access duration must be positive");
    }
    return access((a, b) -> access);
  }

  /**
   * Set a function giving the TTI since last access.
   * <p>
   * Note: Calling this method on a builder with an existing TTI since last access will override the previous value or function.
   *
   * @param access Function giving the TTI since last access
   * @return a new builder with the TTI since last access calculation function
   */
  public <K2 extends K, V2 extends V>ExpiryPolicyBuilder<K2, V2> access(BiFunction<K2, Supplier<? extends V2>, Duration> access) {
    return new ExpiryPolicyBuilder<>(create, Objects.requireNonNull(access), update);
  }

  /**
   * Set TTL since last update.
   * <p>
   * Note: Calling this method on a builder with an existing TTL since last access will override the previous value or function.
   *
   * @param update TTL since last update
   * @return a new builder with the TTL since last update
   */
  public ExpiryPolicyBuilder<K, V> update(Duration update) {
    if (update != null && update.isNegative()) {
      throw new IllegalArgumentException("Update duration must be positive");
    }
    return update((a, b, c) -> update);
  }

  /**
   * Set a function giving the TTL since last update.
   * <p>
   * Note: Calling this method on a builder with an existing TTL since last update will override the previous value or function.
   *
   * @param update Function giving the TTL since last update
   * @return a new builder with the TTL since last update calculation function
   */
  public <K2 extends K, V2 extends V> ExpiryPolicyBuilder<K2, V2> update(TriFunction<K2, Supplier<? extends V2>, V2, Duration> update) {
    return new ExpiryPolicyBuilder<>(create, access, Objects.requireNonNull(update));
  }

  /**
   * Builds an expiry policy instance.
   *
   * @return an {@link ExpiryPolicy}
   */
  public ExpiryPolicy<K, V> build() {
    return new BaseExpiryPolicy<>(create, access, update);
  }

  /**
   * Simple implementation of the {@link ExpiryPolicy} interface allowing to set constants to each expiry types.
   */
  private static class BaseExpiryPolicy<K, V> implements ExpiryPolicy<K, V> {

    private final BiFunction<? super K, ? super V, Duration> create;
    private final BiFunction<? super K, ? super Supplier<? extends V>, Duration> access;
    private final TriFunction<? super K, ? super Supplier<? extends V>, ? super V, Duration> update;

    BaseExpiryPolicy(BiFunction<? super K, ? super V, Duration> create,
                     BiFunction<? super K, ? super Supplier<? extends V>, Duration> access,
                     TriFunction<? super K, ? super Supplier<? extends V>, ? super V, Duration> update) {
      this.create = create;
      this.access = access;
      this.update = update;
    }

    @Override
    public Duration getExpiryForCreation(K key, V value) {
      return create.apply(key, value);
    }

    @Override
    public Duration getExpiryForAccess(K key, Supplier<? extends V> value) {
      return access.apply(key, value);
    }

    @Override
    public Duration getExpiryForUpdate(K key, Supplier<? extends V> oldValue, V newValue) {
      return update.apply(key, oldValue, newValue);
    }
  }

  private static final class TimeToLiveExpiryPolicy extends BaseExpiryPolicy<Object, Object> {
    private final Duration ttl;

    TimeToLiveExpiryPolicy(Duration ttl) {
      super(
        (a, b) -> ttl,
        (a, b) -> null,
        (a, b, c) -> ttl);
      this.ttl = ttl;
    }

    @Override
    public int hashCode() {
      return ttl.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TimeToLiveExpiryPolicy && ttl.equals(((TimeToLiveExpiryPolicy) obj).ttl);
    }

    @Override
    public String toString() {
      return "TTL of " + ttl;
    }
  }

  private static final class TimeToIdleExpiryPolicy extends BaseExpiryPolicy<Object, Object> {
    private final Duration tti;

    TimeToIdleExpiryPolicy(Duration tti) {
      super(
        (a, b) -> tti,
        (a, b) -> tti,
        (a, b, c) -> tti);
      this.tti = tti;
    }

    @Override
    public int hashCode() {
      return tti.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof TimeToIdleExpiryPolicy && tti.equals(((TimeToIdleExpiryPolicy) obj).tti);
    }

    @Override
    public String toString() {
      return "TTI of " + tti;
    }
  }
}
