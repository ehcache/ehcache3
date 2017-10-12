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

import java.util.concurrent.TimeUnit;

/**
 * A time duration in a given {@link TimeUnit}.
 *
 * @see java.time.Duration
 * @see ExpiryPolicy
 *
 * @deprecated Replaced with {@link java.time.Duration}
 */
@Deprecated
public final class Duration {

  /**
   * The infinite {@code Duration}.
   * <p>
   * This constant should be used to express a lack of a concrete expiration time (ie. "eternal").
   */
  public static final Duration INFINITE = new Duration(0, null, true);

  /**
   * The zero {@code Duration}.
   */
  public static final Duration ZERO = new Duration(0, TimeUnit.NANOSECONDS, false);

  /**
   * Convenience method to create a {@code Duration} with the specified values.
   *
   * @param length the duration length
   * @param timeUnit the time unit

   * @return a new {@code Duration}
   *
   * @see #Duration(long, TimeUnit)
   */
  public static Duration of(long length, TimeUnit timeUnit) {
    return new Duration(length, timeUnit);
  }

  private final TimeUnit timeUnit;
  private final long length;

  /**
   * Instantiates a new {@code Duration} of the given length and {@link TimeUnit}.
   *
   * @param length the duration length
   * @param timeUnit the time unit
   *
   * @throws NullPointerException if the given time unit is null
   * @throws IllegalArgumentException if the given length is less than zero
   */
  public Duration(long length, TimeUnit timeUnit) {
    this(length, timeUnit, false);
  }

  private Duration(long length, TimeUnit timeUnit, boolean forever) {
    if (!forever) {
      if (length < 0) {
        throw new IllegalArgumentException("length must be greater than or equal to zero: " + length);
      }

      if (timeUnit == null) {
        throw new NullPointerException("TimeUnit must not be null");
      }
    }

    this.length = length;
    this.timeUnit = timeUnit;
  }

  /**
   * Gets the length of time this {@code Duration} represents.
   *
   * @return the length of this instance
   * @throws IllegalStateException if this instance is {@link #INFINITE}
   *
   * @see #getTimeUnit()
   */
  public long getLength() {
    checkInfinite();
    return length;
  }

  /**
   * Gets the {@link TimeUnit} of this {@code Duration}.
   *
   * @return the {@link TimeUnit} of this instance
   * @throws IllegalStateException if this instance is {@link #INFINITE}
   *
   * @see #getLength()
   */
  public TimeUnit getTimeUnit() {
    checkInfinite();
    return timeUnit;
  }

  private void checkInfinite() {
    if (isInfinite()) {
      throw new IllegalStateException(
          "The calling code should be checking explicitly for Duration#INFINITE or isInfinite()");
    }
  }

  /**
   * Indicates if this duration represents {@link Duration#INFINITE} or an <i>infinite</i> {@code Duration}.
   *
   * @return {@code true} if this instance is the special {@code Forever} value
   */
  public boolean isInfinite() {
    return timeUnit == null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (length ^ (length >>> 32));

    if (length != 0) {
      result = prime * result + ((timeUnit == null) ? 0 : timeUnit.hashCode());
    } else {
      // Differentiate zero from forever
      result = prime * result + ((timeUnit == null) ? 0 : 1);
    }

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }

    if (obj == null) {
      return false;
    }

    if (getClass() != obj.getClass()) {
      return false;
    }

    Duration other = (Duration) obj;
    if (length != other.length) {
      return false;
    }

    if (timeUnit == null || other.timeUnit == null) {
      return timeUnit == other.timeUnit;
    }

    if (timeUnit != other.timeUnit) {
      if (length == 0) {
        return true;
      }
      return false;
    }

    return true;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    if (isInfinite()) {
      return "Duration[INFINITE]";
    }

    if (length == 0) {
      return "Duration[ZERO]";
    }

    return "Duration[length=" + length + ", timeUnit=" + timeUnit.name() + "]";
  }
}
