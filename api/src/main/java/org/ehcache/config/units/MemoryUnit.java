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
 * An enumeration implementing {@link ResourceUnit} to represent memory consumption.
 *
 * @author Ludovic Orban
 */
public enum MemoryUnit implements ResourceUnit {

  /**
   * Bytes unit.
   */
  B {
    /**
     * {@inheritDoc}
     */
    public long toBytes(long size) {
      return size;
    }
  },
  /**
   * Kilobytes unit.
   */
  KB {
    /**
     * {@inheritDoc}
     */
    public long toBytes(long size) {
      return x(size, KILOBYTE / BYTE, MAX / (KILOBYTE / BYTE));
    }
  },
  /**
   * Megabytes unit.
   */
  MB {
    /**
     * {@inheritDoc}
     */
    public long toBytes(long size) {
      return x(size, MEGABYTE / BYTE, MAX / (MEGABYTE / BYTE));
    }
  },
  /**
   * Gigabytes unit.
   */
  GB {
    /**
     * {@inheritDoc}
     */
    public long toBytes(long size) {
      return x(size, GIGABYTE / BYTE, MAX / (GIGABYTE / BYTE));
    }
  },
  /**
   * Terabytes unit.
   */
  TB {
    /**
     * {@inheritDoc}
     */
    public long toBytes(long size) {
      return x(size, TERABYTE / BYTE, MAX / (TERABYTE / BYTE));
    }
  },
  /**
   * Petabytes unit.
   */
  PB {
    /**
     * {@inheritDoc}
     */
    public long toBytes(long size) {
      return x(size, PETABYTE / BYTE, MAX / (PETABYTE / BYTE));
    }
  };

  static final long BYTE = 1L;
  static final long KILOBYTE = 1024L;
  static final long MEGABYTE = KILOBYTE * 1024L;
  static final long GIGABYTE = MEGABYTE * 1024L;
  static final long TERABYTE = GIGABYTE * 1024L;
  static final long PETABYTE = TERABYTE * 1024L;

  static final long MAX = Long.MAX_VALUE;

  /**
   * Scale d by m, checking for overflow.
   * This has a short name to make above code more readable.
   */
  static long x(long d, long m, long over) {
      if (d >  over) return Long.MAX_VALUE;
      if (d < -over) return Long.MIN_VALUE;
      return d * m;
  }

  /**
   * Returns the size in bytes according to the unit this is invoked on.
   *
   * @param size the size, relative to the unit
   * @return the size in bytes
   */
  public long toBytes(long size) {
    throw new AbstractMethodError();
  }
}
