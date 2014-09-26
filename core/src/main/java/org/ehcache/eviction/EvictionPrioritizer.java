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
 *
 * @author cdennis
 */
public enum EvictionPrioritizer implements Comparator<Cache.Entry<?, ?>> {
  
  LRU {
    @Override
    public int compare(Cache.Entry<?, ?> a, Cache.Entry<?, ?> b) {
      return Long.signum(a.getLastAccessTime(TimeUnit.NANOSECONDS) - b.getLastAccessTime(TimeUnit.NANOSECONDS));
    }
  },
  LFU {
    @Override
    public int compare(Cache.Entry<?, ?> a, Cache.Entry<?, ?> b) {
      return Float.compare(a.getHitRate(TimeUnit.NANOSECONDS), b.getHitRate(TimeUnit.NANOSECONDS));
    }
  },
  FIFO {

    @Override
    public int compare(Cache.Entry<?, ?> a, Cache.Entry<?, ?> b) {
      return Long.signum(a.getCreationTime(TimeUnit.NANOSECONDS) - b.getCreationTime(TimeUnit.NANOSECONDS));
    }
  };
}
