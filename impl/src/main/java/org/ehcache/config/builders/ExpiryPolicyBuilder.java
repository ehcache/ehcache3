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
public final class ExpiryPolicyBuilder implements Builder<ExpiryPolicy<Object, Object>>{

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
  public static ExpiryPolicyBuilder expiry() {
    return new ExpiryPolicyBuilder();
  }

  private BiFunction<Object, Object, Duration> create = (key, value) -> ExpiryPolicy.INFINITE;
  private BiFunction<Object, Supplier<? extends Object>, Duration> access = (key, value) -> null;
  private TriFunction<Object, Supplier<? extends Object>, Object, Duration> update = (key, oldValue, newValue) -> null;

  private ExpiryPolicyBuilder() {}

  /**
   * Set TTL since creation
   *
   * @param create TTL since creation
   * @return this builder
   */
  public ExpiryPolicyBuilder create(Duration create) {
    Objects.requireNonNull(create, "Create duration cannot be null");
    if (create.isNegative()) {
      throw new IllegalArgumentException("Create duration must be positive");
    }
    this.create = (a, b) -> create;
    return this;
  }

  /**
   * Set a function giving the TTL since creation
   * @param create Function giving the TTL since creation
   * @return this builder
   */
  public ExpiryPolicyBuilder create(BiFunction<Object, Object, Duration> create) {
    this.create = Objects.requireNonNull(create);
    return this;
  }

  /**
   * Set TTL since last access
   *
   * @param access TTL since last access
   * @return this builder
   */
  public ExpiryPolicyBuilder access(Duration access) {
    if (access != null && access.isNegative()) {
      throw new IllegalArgumentException("Access duration must be positive");
    }
    this.access = (a, b) -> access;
    return this;
  }

  /**
   * Set a function giving the TTL since last access
   * @param access Function giving the TTL since last access
   * @return this builder
   */
  public ExpiryPolicyBuilder access(BiFunction<Object, Supplier<? extends Object>, Duration> access) {
    this.access = Objects.requireNonNull(access);
    return this;
  }

  /**
   * Set TTL since last update
   *
   * @param update TTL since last update
   * @return this builder
   */
  public ExpiryPolicyBuilder update(Duration update) {
    if (update != null && update.isNegative()) {
      throw new IllegalArgumentException("Update duration must be positive");
    }
    this.update = (a, b, c) -> update;
    return this;
  }

  /**
   * Set a function giving the TTL since last update
   * @param update Function giving the TTL since last update
   * @return this builder
   */
  public ExpiryPolicyBuilder update(TriFunction<Object, Supplier<? extends Object>, Object, Duration> update) {
    this.update = Objects.requireNonNull(update);
    return this;
  }

  /**
   *
   * @return an {@link ExpiryPolicy}
   */
  public ExpiryPolicy<Object, Object> build() {
    return new BaseExpiryPolicy(create, access, update);
  }

  /**
   * Simple implementation of the {@link ExpiryPolicy} interface allowing to set constants to each expiry types.
   */
  private static class BaseExpiryPolicy implements ExpiryPolicy<Object, Object> {

    private final BiFunction<Object, Object, Duration> create;
    private final BiFunction<Object, Supplier<? extends Object>, Duration> access;
    private final TriFunction<Object, Supplier<? extends Object>, Object, Duration> update;

    BaseExpiryPolicy(BiFunction<Object, Object, Duration> create,
                     BiFunction<Object, Supplier<? extends Object>, Duration> access,
                     TriFunction<Object, Supplier<? extends Object>, Object, Duration> update) {
      this.create = create;
      this.access = access;
      this.update = update;
    }
    @Override
    public Duration getExpiryForCreation(Object key, Object value) {
      return create.apply(key, value);
    }

    @Override
    public Duration getExpiryForAccess(Object key, Supplier<? extends Object> value) {
      return access.apply(key, value);
    }

    @Override
    public Duration getExpiryForUpdate(Object key, Supplier<? extends Object> oldValue, Object newValue) {
      return update.apply(key, oldValue, newValue);
    }
  }

  private static final class TimeToLiveExpiryPolicy extends BaseExpiryPolicy {
    TimeToLiveExpiryPolicy(Duration ttl) {
      super(
        (a, b) -> ttl,
        (a, b) -> null,
        (a, b, c) -> ttl);
    }
  }

  private static final class TimeToIdleExpiryPolicy extends BaseExpiryPolicy {
    TimeToIdleExpiryPolicy(Duration tti) {
      super(
        (a, b) -> tti,
        (a, b) -> tti,
        (a, b, c) -> tti);
    }
  }
}
