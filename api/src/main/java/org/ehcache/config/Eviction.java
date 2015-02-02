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

package org.ehcache.config;

import org.ehcache.Cache;
import org.ehcache.function.Predicates;

import java.util.concurrent.TimeUnit;

/**
 * Utility class for getting predefined {@link EvictionVeto} and {@link EvictionPrioritizer} instances.
 *
 * @author Alex Snaps
 */
public final class Eviction {

  /**
   * Returns an {@link EvictionVeto} where all mappings are vetoed from eviction.
   *
   * @param <K> the key type on which this veto applies
   * @param <V> the value type on whivh this veto applies
   * @return a veto for all mappings
   */
  public static <K, V> EvictionVeto<K, V> all() {
    return new EvictionVeto<K, V>() {
      @Override
      public boolean test(final Cache.Entry<K, V> argument) {
        return Predicates.<Cache.Entry<K, V>>all().test(argument);
      }
    };
  }

  /**
   * Returns an {@link EvictionVeto} where no mappings are vetoed from eviction.
   *
   * @param <K> the key type on which this veto applies
   * @param <V> the value type on whivh this veto applies
   * @return a veto for no mappings
   */
  public static <K, V> EvictionVeto<K, V> none() {
    return new EvictionVeto<K, V>() {
      @Override
      public boolean test(final Cache.Entry<K, V> argument) {
        return Predicates.<Cache.Entry<K, V>>none().test(argument);
      }
    };
  }

  /**
   * Enumeration holding default {@link EvictionPrioritizer} instances.
   */
  public enum Prioritizer implements EvictionPrioritizer<Object, Object> {

    /**
     * Least Recently Used {@link EvictionPrioritizer}.
     * <p>
     * Ranks eviction candidates by their last access time.  The entry which was
     * last accessed the longest time ago is considered the most eligible for
     * eviction.
     */
    LRU {
      @Override
      public int compare(Cache.Entry<Object, Object> a, Cache.Entry<Object, Object> b) {
        return Long.signum(b.getLastAccessTime(TimeUnit.NANOSECONDS) - a.getLastAccessTime(TimeUnit.NANOSECONDS));
      }
    },

    /**
     * Least Frequently Used {@link EvictionPrioritizer}.
     * <p>
     * Ranks eviction candidates by their frequency of use.  The entry which has
     * the lowest hit rate is considered the most eligible for eviction.
     */
    LFU {
      @Override
      public int compare(Cache.Entry<Object, Object> a, Cache.Entry<Object, Object> b) {
        return Float.compare(b.getHitRate(TimeUnit.NANOSECONDS), a.getHitRate(TimeUnit.NANOSECONDS));
      }
    },

    /**
     * First In, First Out {@link EvictionPrioritizer}.
     * <p>
     * Ranks eviction candidates by their time of creation.  The entry which was
     * created the earliest is considered the most eligible for eviction.
     */
    FIFO {
      @Override
      public int compare(Cache.Entry<Object, Object> a, Cache.Entry<Object, Object> b) {
        return Long.signum(b.getCreationTime(TimeUnit.NANOSECONDS) - a.getCreationTime(TimeUnit.NANOSECONDS));
      }
    };
  }
}
