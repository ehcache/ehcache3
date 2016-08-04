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

package org.ehcache.clustered.client.config;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

/**
 * Describes a timeout value for a clustered operation.  Instances of this class
 * are subject to the operation of the Java {@code java.util.concurrent.TimeUnit} class.
 */
public final class TimeoutDuration {
  private final long amount;
  private final TimeUnit unit;

  private TimeoutDuration(long amount, TimeUnit unit) {
    this.amount = amount;
    this.unit = unit;
  }

  /**
   * Constant indicating no timeout.
   */
  public static final TimeoutDuration NONE = new TimeoutDuration(Long.MAX_VALUE, TimeUnit.NANOSECONDS);

  /**
   * Gets a {@code TimeoutDuration} of the indicated duration.
   *
   * @param amount the non-negative timeout duration
   * @param unit the non-{@code null} units for {@code amount}
   *
   * @return a {@code TimeoutDuration} instance for the specified duration
   *
   * @throws NullPointerException if {@code unit} is {@code null}
   * @throws IllegalArgumentException if {@code amount} is negative
   */
  public static TimeoutDuration of(long amount, TimeUnit unit) {
    if (unit == null) {
      throw new NullPointerException("TimeoutDuration unit can not be null");
    }
    if (amount < 0) {
      throw new IllegalArgumentException("TimeoutDuration time amount must be non-negative");
    }
    if (amount == NONE.amount && unit == NONE.unit) {
      return NONE;
    }
    return new TimeoutDuration(amount, unit);
  }

  /**
   * Converts this {@code TimeoutDuration} to nanoseconds.  Values are converted
   * according to the rules for {@code java.util.concurrent.TimeUnit}.
   *
   * @return the number of nanoseconds represented by this {@code TimeDuration}
   */
  public long toNanos() {
    return unit.toNanos(amount);
  }

  /**
   * Converts this {@code TimeoutDuration} to milliseconds.  Values are converted
   * according to the rules for {@code java.util.concurrent.TimeUnit}.
   *
   * @return the number of milliseconds represented by this {@code TimeDuration}
   */
  public long toMillis() {
    return unit.toMillis(amount);
  }

  /**
   * Converts this {@code TimeDuration} to the specified time unit.  Values are
   * converted according to the rules for {@code java.util.concurrent.TimeUnit}.
   *
   * @param toUnit the {@code TimeUnit} to which this {@code TimeDuration} value is converted
   *
   * @return the duration expressed in {@code toUnit} units
   *
   * @see TimeUnit#convert(long, TimeUnit)
   */
  public long convert(TimeUnit toUnit) {
    return toUnit.convert(amount, unit);
  }

  /**
   * Performs a timed wait on the object provided.
   *
   * @param obj the {@code Object} on which to wait
   *
   * @throws InterruptedException if the wait is interrupted
   *
   * @see TimeUnit#timedWait(Object, long)
   */
  public void timedWait(Object obj) throws InterruptedException {
    unit.timedWait(obj, amount);
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }

    if (other == null || this.getClass() != other.getClass()) {
      return false;
    }

    TimeoutDuration that = (TimeoutDuration)other;
    if (this.amount == 0 && that.amount == 0) {
      return true;          // A duration of 0 is the same in any units
    } else if (this.unit == that.unit) {
      return this.amount == that.amount;
    } else {
      /*
       * Select the more precise/granular unit for the basis of the equals comparison. Since
       * TimeUnit.convert _truncates_ fractional values, attempting to convert one a more
       * precise/granular unit to a less precise/granular unit will result in zero -- indicating
       * which is the more precise/granular unit.
       */
      TimeoutDuration lessGranular;
      TimeoutDuration moreGranular;
      if (this.unit.convert(1, that.unit) > 0) {
        // No truncation -- 'this' has the more granular unit
        lessGranular = that;
        moreGranular = this;
      } else {
        // Truncated -- 'that' has the more granular unit
        lessGranular = this;
        moreGranular = that;
      }
      return BigInteger.valueOf(moreGranular.unit.convert(1, lessGranular.unit))
          .multiply(BigInteger.valueOf(lessGranular.amount))
          .equals(BigInteger.valueOf(moreGranular.amount));
    }
  }

  @Override
  public int hashCode() {
    return BigInteger.valueOf(TimeUnit.NANOSECONDS.convert(1, this.unit)).multiply(BigInteger.valueOf(this.amount)).hashCode();
  }

  @Override
  public String toString() {
    return (this == NONE ? "TimeoutDuration.NONE" : "TimeoutDuration{" + amount + ' ' + unit + '}');
  }
}
