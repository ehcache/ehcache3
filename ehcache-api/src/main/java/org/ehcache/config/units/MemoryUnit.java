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
package org.ehcache.config.units;

import org.ehcache.config.ResourceUnit;

/**
 * A {@link ResourceUnit} that designates memory quantity.
 */
public enum MemoryUnit implements ResourceUnit {

  /**
   * Bytes.
   */
  B("B", 0),
  /**
   * Kilobytes.
   */
  KB("kB", 10),
  /**
   * Megabytes.
   */
  MB("MB", 20),
  /**
   * Gigabytes.
   */
  GB("GB", 30),
  /**
   * Terabytes.
   */
  TB("TB", 40),
  /**
   * Petabytes.
   */
  PB("PB", 50);

  /** the index of this unit */
  private final int index;
  private final String stringForm;

  /** Internal constructor */
  MemoryUnit(String stringForm, int index) {
    this.stringForm = stringForm;
    this.index = index;
  }

  /**
   * Computes <pre>amount * 2^delta</pre>.
   *
   * The result is always rounded toward zero.
   *
   * @param delta log<sub>2</sub>(divisor)
   * @param amount dividend
   * @throws ArithmeticException if the result overflows
   */
  private static long doConvert(int delta, long amount) throws ArithmeticException {
    if (delta == 0 || amount == 0) {
      return amount;
    } else if (delta < 0) {
      // Hacker's Delight : 10-1
      long t = amount >> (-delta - 1);
      t >>>= 64 + delta;
      t += amount;
      return t >> -delta;
    } else if (delta >= Long.numberOfLeadingZeros(amount < 0 ? ~amount : amount)) {
      throw new ArithmeticException("Conversion overflows");
    } else {
      return amount << delta;
    }
  }

  /**
   * Converts {@code quantity} in this unit to bytes.
   *
   * @param quantity the quantity
   * @return the quantity in bytes
   */
  public long toBytes(long quantity) {
    return doConvert(index - B.index, quantity);
  }

  /**
   * Converts {@code quantity} in {@code unit} into this unit.
   *
   * @param quantity quantity to convert
   * @param unit {@code quantity}'s unit
   * @return the quantity in this unit
   */
  public long convert(long quantity, MemoryUnit unit) {
    return doConvert(unit.index - index, quantity);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return stringForm;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int compareTo(long thisSize, long thatSize, ResourceUnit thatUnit) throws IllegalArgumentException {
    if (thatUnit instanceof MemoryUnit) {
      MemoryUnit mThatUnit = (MemoryUnit) thatUnit;
      if (index < mThatUnit.index) {
        try {
          return Long.signum(thisSize - convert(thatSize, mThatUnit));
        } catch (ArithmeticException e) {
          return Long.signum(mThatUnit.convert(thisSize, this) - thatSize);
        }
      } else {
        try {
          return Long.signum(mThatUnit.convert(thisSize, this) - thatSize);
        } catch (ArithmeticException e) {
          return Long.signum(thisSize - convert(thatSize, mThatUnit));
        }
      }
    } else {
      throw new IllegalArgumentException();
    }
  }
}
