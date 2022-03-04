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

import org.terracotta.offheapstore.buffersource.BufferSource;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.buffersource.TimingBufferSource;

import java.util.concurrent.TimeUnit;

/**
 * OffHeapStoreUtils
 */
public final class OffHeapStoreUtils {

  static final String PATH_PREFIX = "org.ehcache.offheap.config.";

  /* threshold in ms for a chunk allocation over which a warning will be logged */
  private static final long SLOW_DELAY = 3000L;
  private static final String SLOW_DELAY_PROPERTY = "slowAllocationDelay";
  /* threshold in ms for a chunk allocation over which an attempt may be made to abort the VM */
  private static final long CRITICAL_DELAY = 30000L;
  private static final String CRITICAL_DELAY_PROPERTY = "criticalAllocationDelay";
  /* halt the VM when critical allocation delay is reached */
  private static final boolean HALT_ON_CRITICAL_DELAY = true;
  private static final String HALT_ON_CRITICAL_DELAY_PROPERTY = "haltOnCriticalAllocationDelay";

  private OffHeapStoreUtils() {}

  public static BufferSource getBufferSource() {
    long slowDelay = getAdvancedLongConfigProperty(SLOW_DELAY_PROPERTY, SLOW_DELAY);
    long critDelay = getAdvancedLongConfigProperty(CRITICAL_DELAY_PROPERTY, CRITICAL_DELAY);
    boolean haltOnCrit = getAdvancedBooleanConfigProperty(HALT_ON_CRITICAL_DELAY_PROPERTY, HALT_ON_CRITICAL_DELAY);
    return new TimingBufferSource(new OffHeapBufferSource(), slowDelay, TimeUnit.MILLISECONDS, critDelay, TimeUnit.MILLISECONDS, haltOnCrit);
  }

  public static long getAdvancedMemorySizeConfigProperty(String property, long defaultValue) {
    String globalPropertyKey = PATH_PREFIX + property;
    return MemorySizeParser.parse(System.getProperty(globalPropertyKey, Long.toString(defaultValue)));
  }

  public static long getAdvancedLongConfigProperty(String property, long defaultValue) {
    String globalPropertyKey = PATH_PREFIX + property;
    return Long.parseLong(System.getProperty(globalPropertyKey, Long.toString(defaultValue)));
  }

  public static boolean getAdvancedBooleanConfigProperty(String property, boolean defaultValue) {
    String globalPropertyKey = PATH_PREFIX + property;
    return Boolean.parseBoolean(System.getProperty(globalPropertyKey, Boolean.toString(defaultValue)));
  }
}
