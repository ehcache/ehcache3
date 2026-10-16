/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2026
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

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package org.ehcache.impl.internal.concurrent;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Util class providing a per-thread probe, standing in for the one {@code ThreadLocalRandom} keeps in
 * {@code Thread}, which cannot be accessed outside of {@code java.base}.
 */
class ThreadLocalRandomUtil {

  // Same seeding as ThreadLocalRandom.localInit
  private static final int PROBE_INCREMENT = 0x9e3779b9;
  private static final AtomicInteger PROBE_GENERATOR = new AtomicInteger();

  // An int[] value keeps thread local map entries from referencing this class loader
  private static final ThreadLocal<int[]> PROBE = ThreadLocal.withInitial(() -> new int[1]);

  static final int getProbe() {
    return PROBE.get()[0];
  }

  static final int advanceProbe(int probe) {
    probe ^= probe << 13;   // xorshift
    probe ^= probe >>> 17;
    probe ^= probe << 5;
    PROBE.get()[0] = probe;
    return probe;
  }

  static final void localInit() {
    int p = PROBE_GENERATOR.addAndGet(PROBE_INCREMENT);
    PROBE.get()[0] = (p == 0) ? 1 : p; // skip 0
  }

  /** Optimized form of: key + "=" + val */
  static String mapEntryToString(Object key, Object val) {
    final String k, v;
    final int klen, vlen;
    final char[] chars =
      new char[(klen = (k = objectToString(key)).length()) +
               (vlen = (v = objectToString(val)).length()) + 1];
    k.getChars(0, klen, chars, 0);
    chars[klen] = '=';
    v.getChars(0, vlen, chars, klen + 1);
    return new String(chars);
  }

  private static String objectToString(Object x) {
    // Extreme compatibility with StringBuilder.append(null)
    String s;
    return (x == null || (s = x.toString()) == null) ? "null" : s;
  }
}
