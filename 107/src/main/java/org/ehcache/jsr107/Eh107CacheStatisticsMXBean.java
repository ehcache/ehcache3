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
package org.ehcache.jsr107;

import java.util.EnumMap;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.cache.management.CacheStatisticsMXBean;

import org.ehcache.statistics.BulkOps;
import org.ehcache.statistics.CacheStatistics;

/**
 * @author teck
 */
class Eh107CacheStatisticsMXBean extends Eh107MXBean implements CacheStatisticsMXBean {

  private final org.ehcache.statistics.CacheStatistics statistics;
  private final ConcurrentMap<BulkOps, AtomicLong> bulkMethodCounts;
  private final EnumMap<Stat, AtomicLong> baselines = new EnumMap<Stat, AtomicLong>(Stat.class);

  Eh107CacheStatisticsMXBean(String cacheName, Eh107CacheManager cacheManager,
      org.ehcache.statistics.CacheStatistics statistics) {
    super(cacheName, cacheManager, "CacheStatistics");
    this.statistics = statistics;
    bulkMethodCounts = statistics.getBulkMethodEntries();

    for (Stat s : Stat.values()) {
      baselines.put(s, new AtomicLong());
    }
  }

  @Override
  public void clear() {
    // snapshot all the counters
    for (Entry<Stat, AtomicLong> entry : baselines.entrySet()) {
      entry.getValue().set(entry.getKey().snapshot(statistics));
    }
  }

  @Override
  public long getCacheHits() {
    return minZero(statistics.getCacheHits() - baselineValue(Stat.GETS));
  }

  @Override
  public float getCacheHitPercentage() {
    return zeroForNaN(statistics.getCacheHitPercentage());
  }

  @Override
  public long getCacheMisses() {
    return minZero(statistics.getCacheMisses() - baselineValue(Stat.MISSES));
  }

  @Override
  public float getCacheMissPercentage() {
    return zeroForNaN(statistics.getCacheMissPercentage());
  }

  @Override
  public long getCacheGets() {
    return minZero((getBulkCount(BulkOps.GET_ALL) - baselineValue(Stat.BULK_GETS))
        + (statistics.getCacheGets() - baselineValue(Stat.GETS)));
  }

  @Override
  public long getCachePuts() {
    return minZero((getBulkCount(BulkOps.PUT_ALL) - baselineValue(Stat.BULK_PUTS))
        + (statistics.getCachePuts() - baselineValue(Stat.PUTS)));
  }

  @Override
  public long getCacheRemovals() {
    return minZero((getBulkCount(BulkOps.REMOVE_ALL) - baselineValue(Stat.BULK_REMOVES))
        + (statistics.getCacheRemovals() - baselineValue(Stat.REMOVALS)));
  }

  @Override
  public long getCacheEvictions() {
    return minZero(statistics.getCacheEvictions() - baselineValue(Stat.EVICTIONS));
  }

  @Override
  public float getAverageGetTime() {
    return zeroForNaN(statistics.getAverageGetTime());
  }

  @Override
  public float getAveragePutTime() {
    return zeroForNaN(statistics.getAveragePutTime());
  }

  @Override
  public float getAverageRemoveTime() {
    return zeroForNaN(statistics.getAverageRemoveTime());
  }

  private long getBulkCount(BulkOps op) {
    AtomicLong counter = bulkMethodCounts.get(op);
    return counter == null ? 0L : counter.get();
  }

  private long baselineValue(Stat stat) {
    return baselines.get(stat).get();
  }

  private static long minZero(long value) {
    return Math.max(0, value);
  }

  private static float zeroForNaN(float value) {
    if (Float.isNaN(value)) {
      return 0F;
    }
    return value;
  }

  private enum Stat {
    EVICTIONS {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getCacheEvictions();
      }
    },
    GETS {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getCacheGets();
      }
    },
    HITS {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getCacheHits();
      }
    },
    MISSES {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getCacheMisses();
      }
    },
    PUTS {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getCachePuts();
      }
    },
    REMOVALS {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getCacheRemovals();
      }
    },
    BULK_GETS {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getBulkMethodEntries().get(BulkOps.GET_ALL).get();
      }
    },
    BULK_PUTS {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getBulkMethodEntries().get(BulkOps.PUT_ALL).get();
      }
    },
    BULK_REMOVES {
      @Override
      long snapshot(CacheStatistics stats) {
        return stats.getBulkMethodEntries().get(BulkOps.REMOVE_ALL).get();
      }
    };

    abstract long snapshot(CacheStatistics stats);
  }

}
