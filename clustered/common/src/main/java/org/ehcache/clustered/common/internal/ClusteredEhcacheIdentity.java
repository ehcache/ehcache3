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

package org.ehcache.clustered.common.internal;

import java.util.UUID;

public final class ClusteredEhcacheIdentity {

  private ClusteredEhcacheIdentity() {}

  public static byte[] serialize(UUID identity) {
    long msb = identity.getMostSignificantBits();
    long lsb = identity.getLeastSignificantBits();
    byte[] bytes = new byte[16];
    putLong(bytes, 0, msb);
    putLong(bytes, 8, lsb);
    return bytes;
  }

  public static UUID deserialize(byte[] bytes) {
    if (bytes.length != 16) {
      throw new IllegalArgumentException("Expected a 16 byte (UUID) config stream");
    }
    long msb = getLong(bytes, 0);
    long lsb = getLong(bytes, 8);
    return new UUID(msb, lsb);
  }

  private static void putLong(byte[] array, int offset, long value) {
    if (offset < 0 || array.length < offset + 8) {
      throw new ArrayIndexOutOfBoundsException();
    } else {
      for (int i = 7; i >= 0; i--) {
        array[offset + i] = (byte) value;
        value >>= 8;
      }
    }
  }

  private static long getLong(byte[] array, int offset) {
    if (offset < 0 || array.length < offset + 8) {
      throw new ArrayIndexOutOfBoundsException();
    } else {
      long result = 0;
      for (int i = 0; i < 8; i++) {
        result <<= 8;
        result |= array[offset + i] & 0xff;
      }
      return result;
    }
  }
}
