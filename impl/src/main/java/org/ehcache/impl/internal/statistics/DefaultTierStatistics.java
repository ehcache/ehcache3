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

package org.ehcache.impl.internal.statistics;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.ehcache.Cache;
import org.ehcache.core.statistics.StoreOperationOutcomes;
import org.ehcache.core.statistics.TierOperationOutcomes;
import org.ehcache.core.statistics.TierStatistics;
import org.ehcache.core.statistics.TypedValueStatistic;
import org.terracotta.statistics.ConstantValueStatistic;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.extended.StatisticType;

import static org.ehcache.impl.internal.statistics.StatsUtils.findStatisticOnDescendants;

/**
 * Contains usage statistics relative to a given tier.
 */
class DefaultTierStatistics implements TierStatistics {

  private static final ValueStatistic<Long> NOT_AVAILABLE = ConstantValueStatistic.instance(-1L);

  private volatile CompensatingCounters compensatingCounters = CompensatingCounters.empty();

  private final Map<String, TypedValueStatistic> knownStatistics;

  private final OperationStatistic<TierOperationOutcomes.GetOutcome> get;
  private final OperationStatistic<StoreOperationOutcomes.PutOutcome> put;
  private final OperationStatistic<StoreOperationOutcomes.RemoveOutcome> remove;
  private final OperationStatistic<TierOperationOutcomes.EvictionOutcome> eviction;
  private final OperationStatistic<StoreOperationOutcomes.ExpirationOutcome> expiration;
  private final ValueStatistic<Long> mapping;
  private final ValueStatistic<Long> maxMapping;
  private final ValueStatistic<Long> allocatedMemory;
  private final ValueStatistic<Long> occupiedMemory;

  public DefaultTierStatistics(Cache<?, ?> cache, String tierName) {
    get = findStatisticOnDescendants(cache, tierName, "tier", "get");
    put = findStatisticOnDescendants(cache, tierName, "put");
    remove = findStatisticOnDescendants(cache, tierName, "remove");
    eviction = findStatisticOnDescendants(cache, tierName, "tier", "eviction");
    expiration = findStatisticOnDescendants(cache, tierName, "expiration");
    mapping = findValueStatistics(cache, tierName, "mappings");
    maxMapping = findValueStatistics(cache, tierName, "maxMappings");
    allocatedMemory = findValueStatistics(cache, tierName, "allocatedMemory");
    occupiedMemory = findValueStatistics(cache, tierName, "occupiedMemory");

    Map<String, TypedValueStatistic> knownStatistics = createKnownStatistics(tierName);
    this.knownStatistics = Collections.unmodifiableMap(knownStatistics);
  }

  private Map<String, TypedValueStatistic> createKnownStatistics(String tierName) {
    Map<String, TypedValueStatistic> knownStatistics = new HashMap<String, TypedValueStatistic>(7);
    addKnownStatistic(knownStatistics, tierName, "HitCount", get, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getHits();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "MissCount", get, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getMisses();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "PutCount", get, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getMisses();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "UpdateCount", get, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getMisses();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "RemovalCount", get, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getMisses();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "EvictionCount", get, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getEvictions();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "ExpirationCount", get, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getExpirations();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "MappingCount", mapping, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getMappings();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "MaxMappingCount", maxMapping, new TypedValueStatistic(StatisticType.COUNTER) {
      @Override
      public Number value() {
        return getMaxMappings();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "AllocatedByteSize", allocatedMemory, new TypedValueStatistic(StatisticType.SIZE) {
      @Override
      public Number value() {
        return getAllocatedByteSize();
      }
    });
    addKnownStatistic(knownStatistics, tierName, "OccupiedByteSize", occupiedMemory, new TypedValueStatistic(StatisticType.SIZE) {
      @Override
      public Number value() {
        return getOccupiedByteSize();
      }
    });
    return knownStatistics;
  }

  public Map<String, TypedValueStatistic> getKnownStatistics() {
    return knownStatistics;
  }

  private static void addKnownStatistic(Map<String, TypedValueStatistic> knownStatistics, String tierName, String name, Object stat, TypedValueStatistic statistic) {
    if (stat != NOT_AVAILABLE) {
      knownStatistics.put(tierName + ":" + name, statistic);
    }
  }

  private ValueStatistic<Long> findValueStatistics(Cache<?, ?> cache, String tierName, String statName) {
    ValueStatistic<Long> stat = findStatisticOnDescendants(cache, tierName, tierName, statName);
    if (stat == null) {
      return NOT_AVAILABLE;
    }
    return stat;
  }

  /**
   * Reset the values for this tier. However, note that {@code mapping, maxMappings, allocatedMemory, occupiedMemory}
   * but be reset since it doesn't make sense.
   */
  public void clear() {
    compensatingCounters = compensatingCounters.snapshot(this);
  }

  public long getHits() {
    return get.sum(EnumSet.of(TierOperationOutcomes.GetOutcome.HIT)) -
           compensatingCounters.hits;
  }

  public long getMisses() {
    return get.sum(EnumSet.of(TierOperationOutcomes.GetOutcome.MISS)) -
           compensatingCounters.misses;
  }

  public long getPuts() {
    return put.sum(EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT)) +
           put.sum(EnumSet.of(StoreOperationOutcomes.PutOutcome.REPLACED)) -
           compensatingCounters.puts;
  }

  public long getUpdates() {
    return put.sum(EnumSet.of(StoreOperationOutcomes.PutOutcome.REPLACED)) -
           compensatingCounters.updates;
  }

  public long getRemovals() {
    return remove.sum(EnumSet.of(StoreOperationOutcomes.RemoveOutcome.REMOVED)) -
           compensatingCounters.removals;
  }

  public long getEvictions() {
    return eviction.sum(EnumSet.of(TierOperationOutcomes.EvictionOutcome.SUCCESS)) -
      compensatingCounters.evictions;
  }

  public long getExpirations() {
    return expiration.sum() - compensatingCounters.expirations;
  }

  public long getMappings() {
    return mapping.value();
  }

  public long getMaxMappings() {
    return maxMapping.value();
  }

  public long getAllocatedByteSize() {
    return allocatedMemory.value();
  }

  public long getOccupiedByteSize() {
    return occupiedMemory.value();
  }

  private static class CompensatingCounters {
    final long hits;
    final long misses;
    final long puts;
    final long updates;
    final long removals;
    final long evictions;
    final long expirations;

    private CompensatingCounters(long hits, long misses, long puts, long updates, long removals, long evictions, long expirations) {
      this.hits = hits;
      this.misses = misses;
      this.puts = puts;
      this.updates = updates;
      this.removals = removals;
      this.evictions = evictions;
      this.expirations = expirations;
    }

    static CompensatingCounters empty() {
      return new CompensatingCounters(0, 0, 0, 0, 0, 0, 0);
    }

    CompensatingCounters snapshot(DefaultTierStatistics statistics) {
      return new CompensatingCounters(
        statistics.getHits() + hits,
        statistics.getMisses() + misses,
        statistics.getPuts() + puts,
        statistics.getUpdates() + updates,
        statistics.getRemovals() + removals,
        statistics.getEvictions() + evictions,
        statistics.getExpirations() + expirations
      );
    }
  }
}
