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

public final class Duration {

  public static final Duration FOREVER = new Duration(0, null, true);
  
  private final TimeUnit timeUnit;
  private final long amount;

  public Duration(long amount, TimeUnit timeUnit) {
    this(amount, timeUnit, false);
  }

  private Duration(long amount, TimeUnit timeUnit, boolean forever) {
    if (!forever) {
      if (amount <= 0) {
        throw new IllegalArgumentException("amount must be greater than zero: " + amount);
      }

      if (timeUnit == null) {
        throw new NullPointerException("TimeUnit must not be null");
      }
    }

    this.amount = amount;
    this.timeUnit = timeUnit;
  }
  
  public long getAmount() {
    checkForever();
    return amount;
  }
  
  public TimeUnit getTimeUnit() {
    checkForever();
    return timeUnit;
  }

  private void checkForever() {
    if (isForever()) {
      throw new IllegalStateException(
          "The calling code should be checking explicitly for Duration#FOREVER or isForever()");
    }
  }

  public boolean isForever() {
    return timeUnit == null;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + (int) (amount ^ (amount >>> 32));
    result = prime * result + ((timeUnit == null) ? 0 : timeUnit.hashCode());
    return result;
  }

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
    if (amount != other.amount) {
      return false;
    }

    if (timeUnit != other.timeUnit) {
      if (amount == 0) {
        return true;
      }
      return false;
    }

    return true;
  }

  @Override
  public String toString() {
    if (isForever()) {
      return "Duration[FOREVER]";
    }

    if (amount == 0) {
      return "Duration[ZERO]";
    }

    return "Duration[amount=" + amount + ", timeUnit=" + timeUnit.name() + "]";
  }
}
