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

package org.ehcache.eviction;

import java.util.Comparator;
import java.util.concurrent.TimeUnit;
import org.ehcache.Cache;

/**
 * The set of default eviction prioritization algorithms.
 * 
 * @author Chris Dennis
 */
public enum EvictionPrioritizer implements Comparator<Cache.Entry<?, ?>> {

  /**
   * Least Recently Used Eviction Prioritizer.
   * <p>
   * Ranks eviction candidates by their last access time.  The entry which was
   * last accessed the longest time ago is considered the most eligible for
   * eviction.
   */
  LRU {
    @Override
    public int compare(Cache.Entry<?, ?> a, Cache.Entry<?, ?> b) {
      return Long.signum(b.getLastAccessTime(TimeUnit.NANOSECONDS) - a.getLastAccessTime(TimeUnit.NANOSECONDS));
    }
  },
  
  /**
   * Least Frequently Used Eviction Prioritizer.
   * <p>
   * Ranks eviction candidates by their frequency of use.  The entry which has
   * the lowest hit rate is considered the most eligible for eviction.
   */
  LFU {
    @Override
    public int compare(Cache.Entry<?, ?> a, Cache.Entry<?, ?> b) {
      return Float.compare(b.getHitRate(TimeUnit.NANOSECONDS), a.getHitRate(TimeUnit.NANOSECONDS));
    }
  },
  
  /**
   * First In, First Out Eviction Prioritizer.
   * <p>
   * Ranks eviction candidates by their time of creation.  The entry which was
   * created the earliest is considered the most eligible for eviction.
   */
  FIFO {
    @Override
    public int compare(Cache.Entry<?, ?> a, Cache.Entry<?, ?> b) {
      return Long.signum(b.getCreationTime(TimeUnit.NANOSECONDS) - a.getCreationTime(TimeUnit.NANOSECONDS));
    }
  };
}
