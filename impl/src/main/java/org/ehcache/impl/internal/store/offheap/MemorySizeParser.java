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

package org.ehcache.impl.internal.store.offheap;

/**
 * Memory size parser using the letter k or K to indicate kilobytes, the letter m or M to indicate megabytes,
 * the letter g or G to indicate gigabytes and the letter t or T to indicate terabytes.
 */
public class MemorySizeParser {
  private static final long BYTE = 1;
  private static final long KILOBYTE = 1024;
  private static final long MEGABYTE = 1024 * KILOBYTE;
  private static final long GIGABYTE = 1024 * MEGABYTE;
  private static final long TERABYTE = 1024 * GIGABYTE;

  /**
   * Parse a String containing a human-readable memory size.
   *
   * @param configuredMemorySize the String containing a human-readable memory size.
   * @return the memory size in bytes.
   * @throws IllegalArgumentException thrown when the configured memory size cannot be parsed.
   */
  public static long parse(String configuredMemorySize) throws IllegalArgumentException {
    MemorySize size = parseIncludingUnit(configuredMemorySize);
    return size.calculateMemorySizeInBytes();
  }

  private static MemorySize parseIncludingUnit(String configuredMemorySize) throws IllegalArgumentException {
    if (configuredMemorySize == null || "".equals(configuredMemorySize)) {
      return new MemorySize("0", BYTE);
    }

    char unit = configuredMemorySize.charAt(configuredMemorySize.length() - 1);
    MemorySize memorySize;

    switch (unit) {
      case 'k':
      case 'K':
        memorySize = toMemorySize(configuredMemorySize, KILOBYTE);
        break;
      case 'm':
      case 'M':
        memorySize = toMemorySize(configuredMemorySize, MEGABYTE);
        break;
      case 'g':
      case 'G':
        memorySize = toMemorySize(configuredMemorySize, GIGABYTE);
        break;
      case 't':
      case 'T':
        memorySize = toMemorySize(configuredMemorySize, TERABYTE);
        break;
      default:
        try {
          Integer.parseInt("" + unit);
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("invalid format for memory size [" + configuredMemorySize + "]");
        }
        memorySize = new MemorySize(configuredMemorySize, BYTE);
    }

    return memorySize;
  }

  private static MemorySize toMemorySize(String configuredMemorySize, long unitMultiplier) {
    if (configuredMemorySize.length() < 2) {
      throw new IllegalArgumentException("invalid format for memory size [" + configuredMemorySize + "]");
    }
    return new MemorySize(configuredMemorySize.substring(0, configuredMemorySize.length() - 1), unitMultiplier);
  }

  /**
   * Memory size calculator.
   */
  private static final class MemorySize {
    private String configuredMemorySizeWithoutUnit;
    private long multiplicationFactor;

    private MemorySize(String configuredMemorySizeWithoutUnit, long multiplicationFactor) {
      this.configuredMemorySizeWithoutUnit = configuredMemorySizeWithoutUnit;
      this.multiplicationFactor = multiplicationFactor;
    }

    public long calculateMemorySizeInBytes() throws IllegalArgumentException {
      try {
        long memorySizeLong = Long.parseLong(configuredMemorySizeWithoutUnit);
        long result = memorySizeLong * multiplicationFactor;
        if (result < 0) {
          throw new IllegalArgumentException("memory size cannot be negative");
        }
        return result;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("invalid format for memory size");
      }
    }
  }
}
