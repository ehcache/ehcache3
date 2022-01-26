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

package org.ehcache.core.statistics;

import org.ehcache.Cache;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.ZeroOperationStatistic;

import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static org.ehcache.core.statistics.StatsUtils.findStatisticOnDescendants;
import static org.terracotta.statistics.ValueStatistics.counter;
import static org.terracotta.statistics.ValueStatistics.gauge;

/**
 * Contains usage statistics relative to a given tier.
 */
public class DefaultTierStatistics implements TierStatistics {

  private volatile CompensatingCounters compensatingCounters = CompensatingCounters.empty();

  private final Map<String, ValueStatistic<?>> knownStatistics;

  private final OperationStatistic<TierOperationOutcomes.GetOutcome> get;
  private final OperationStatistic<StoreOperationOutcomes.PutOutcome> put;
  private final OperationStatistic<StoreOperationOutcomes.PutIfAbsentOutcome> putIfAbsent;
  private final OperationStatistic<StoreOperationOutcomes.ReplaceOutcome> replace;
  private final OperationStatistic<StoreOperationOutcomes.ConditionalReplaceOutcome> conditionalReplace;
  private final OperationStatistic<StoreOperationOutcomes.RemoveOutcome> remove;
  private final OperationStatistic<StoreOperationOutcomes.ConditionalRemoveOutcome> conditionalRemove;
  private final OperationStatistic<TierOperationOutcomes.EvictionOutcome> eviction;
  private final OperationStatistic<StoreOperationOutcomes.ExpirationOutcome> expiration;
  private final OperationStatistic<StoreOperationOutcomes.ComputeOutcome> compute;
  private final OperationStatistic<StoreOperationOutcomes.ComputeIfAbsentOutcome> computeIfAbsent;

  //Ehcache default to -1 if unavailable, but the management layer needs optional or null
  // (since -1 can be a normal value for a stat).
  private final Optional<ValueStatistic<Long>> mapping;
  private final Optional<ValueStatistic<Long>> allocatedMemory;
  private final Optional<ValueStatistic<Long>> occupiedMemory;

  public DefaultTierStatistics(Cache<?, ?> cache, String tierName) {

    get = findOperationStatistic(cache, tierName, "tier", "get");
    put = findOperationStatistic(cache, tierName, "put");
    putIfAbsent = findOperationStatistic(cache, tierName, "putIfAbsent");
    replace = findOperationStatistic(cache, tierName, "replace");
    conditionalReplace = findOperationStatistic(cache, tierName, "conditionalReplace");
    remove = findOperationStatistic(cache, tierName, "remove");
    conditionalRemove = findOperationStatistic(cache, tierName, "conditionalRemove");
    eviction = findOperationStatistic(cache, tierName, "tier", "eviction");
    expiration = findOperationStatistic(cache, tierName, "expiration");
    compute = findOperationStatistic(cache, tierName, "compute");
    computeIfAbsent = findOperationStatistic(cache, tierName, "computeIfAbsent");

    mapping = findValueStatistics(cache, tierName, "mappings");
    allocatedMemory = findValueStatistics(cache, tierName, "allocatedMemory");
    occupiedMemory = findValueStatistics(cache, tierName, "occupiedMemory");

    Map<String, ValueStatistic<?>> knownStatistics = createKnownStatistics(tierName);
    this.knownStatistics = Collections.unmodifiableMap(knownStatistics);
  }

  private Map<String, ValueStatistic<?>> createKnownStatistics(String tierName) {
    Map<String, ValueStatistic<?>> knownStatistics = new HashMap<>(7);
    addIfPresent(knownStatistics, tierName + ":HitCount", get, this::getHits);
    addIfPresent(knownStatistics, tierName + ":MissCount", get, this::getMisses);
    addIfPresent(knownStatistics, tierName + ":PutCount", put, this::getPuts);
    addIfPresent(knownStatistics, tierName + ":RemovalCount", remove, this::getRemovals);

    // These two a special because they are used by the cache so they should always be there
    knownStatistics.put(tierName + ":EvictionCount", counter(this::getEvictions));
    knownStatistics.put(tierName + ":ExpirationCount", counter(this::getExpirations));

    mapping.ifPresent(longValueStatistic -> knownStatistics.put(tierName + ":MappingCount", gauge(this::getMappings)));
    allocatedMemory.ifPresent(longValueStatistic -> knownStatistics.put(tierName + ":AllocatedByteSize", gauge(this::getAllocatedByteSize)));
    occupiedMemory.ifPresent(longValueStatistic -> knownStatistics.put(tierName + ":OccupiedByteSize", gauge(this::getOccupiedByteSize)));
    return knownStatistics;
  }

  /**
   * Add the statistic as a known statistic only if the reference statistic is available. We consider that the reference statistic can only be
   * an instance of {@code ZeroOperationStatistic} when statistics are disabled.
   *
   * @param knownStatistics map of known statistics
   * @param name the name of the statistic to add
   * @param reference the reference statistic that should be available for the statistic to be added
   * @param valueSupplier the supplier that will provide the current value for the statistic
   * @param <T> type of the supplied value
   */
  private static <T extends Number> void addIfPresent(Map<String, ValueStatistic<?>> knownStatistics, String name, OperationStatistic<?> reference, Supplier<T> valueSupplier) {
    if(!(reference instanceof ZeroOperationStatistic)) {
      knownStatistics.put(name, counter(valueSupplier));
    }
  }

  @Override
  public Map<String, ValueStatistic<?>> getKnownStatistics() {
    return knownStatistics;
  }

  private <T extends Enum<T>> OperationStatistic<T> findOperationStatistic(Cache<?, ?> cache, String tierName, String tag, String stat) {
    return StatsUtils.<OperationStatistic<T>>findStatisticOnDescendants(cache, tierName, tag, stat).orElse(ZeroOperationStatistic.get());
  }

  private <T extends Enum<T>> OperationStatistic<T> findOperationStatistic(Cache<?, ?> cache, String tierName, String stat) {
    return StatsUtils.<OperationStatistic<T>>findStatisticOnDescendants(cache, tierName, stat).orElse(ZeroOperationStatistic.get());
  }

  private Optional<ValueStatistic<Long>> findValueStatistics(Cache<?, ?> cache, String tierName, String statName) {
    return findStatisticOnDescendants(cache, tierName, statName);
  }

  /**
   * Reset the values for this tier. However, note that {@code mapping, allocatedMemory, occupiedMemory}
   * but be reset since it doesn't make sense.
   */
  @Override
  public void clear() {
    compensatingCounters = compensatingCounters.snapshot(this);
  }

  @Override
  public long getHits() {
    return get.sum(EnumSet.of(TierOperationOutcomes.GetOutcome.HIT)) +
           putIfAbsent.sum(EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.HIT)) +
           replace.sum(EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.REPLACED)) +
           compute.sum(EnumSet.of(StoreOperationOutcomes.ComputeOutcome.HIT)) +
           computeIfAbsent.sum(EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.HIT)) +
           conditionalReplace.sum(EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED)) +
           conditionalRemove.sum(EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED)) -
           compensatingCounters.hits;
  }

  @Override
  public long getMisses() {
    return get.sum(EnumSet.of(TierOperationOutcomes.GetOutcome.MISS)) +
           putIfAbsent.sum(EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT)) +
           replace.sum(EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.MISS)) +
           computeIfAbsent.sum(EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.NOOP)) +
           conditionalReplace.sum(EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.MISS)) +
           conditionalRemove.sum(EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.MISS)) -
           compensatingCounters.misses;
  }

  @Override
  public long getPuts() {
    return put.sum(EnumSet.of(StoreOperationOutcomes.PutOutcome.PUT)) +
           putIfAbsent.sum(EnumSet.of(StoreOperationOutcomes.PutIfAbsentOutcome.PUT)) +
           compute.sum(EnumSet.of(StoreOperationOutcomes.ComputeOutcome.PUT)) +
           computeIfAbsent.sum(EnumSet.of(StoreOperationOutcomes.ComputeIfAbsentOutcome.PUT)) +
           replace.sum(EnumSet.of(StoreOperationOutcomes.ReplaceOutcome.REPLACED)) +
           conditionalReplace.sum(EnumSet.of(StoreOperationOutcomes.ConditionalReplaceOutcome.REPLACED)) -
           compensatingCounters.puts;
  }

  @Override
  public long getRemovals() {
    return remove.sum(EnumSet.of(StoreOperationOutcomes.RemoveOutcome.REMOVED)) +
           compute.sum(EnumSet.of(StoreOperationOutcomes.ComputeOutcome.REMOVED)) +
           conditionalRemove.sum(EnumSet.of(StoreOperationOutcomes.ConditionalRemoveOutcome.REMOVED)) -
           compensatingCounters.removals;
  }

  @Override
  public long getEvictions() {
    return eviction.sum(EnumSet.of(TierOperationOutcomes.EvictionOutcome.SUCCESS)) -
      compensatingCounters.evictions;
  }

  @Override
  public long getExpirations() {
    return expiration.sum() - compensatingCounters.expirations;
  }

  @Override
  public long getMappings() {
    return mapping.map(ValueStatistic::value).orElse(-1L);
  }

  @Override
  public long getAllocatedByteSize() {
    return allocatedMemory.map(ValueStatistic::value).orElse(-1L);
  }

  @Override
  public long getOccupiedByteSize() {
    return occupiedMemory.map(ValueStatistic::value).orElse(-1L);
  }

  private static class CompensatingCounters {
    final long hits;
    final long misses;
    final long puts;
    final long removals;
    final long evictions;
    final long expirations;

    private CompensatingCounters(long hits, long misses, long puts, long removals, long evictions, long expirations) {
      this.hits = hits;
      this.misses = misses;
      this.puts = puts;
      this.removals = removals;
      this.evictions = evictions;
      this.expirations = expirations;
    }

    static CompensatingCounters empty() {
      return new CompensatingCounters(0, 0, 0, 0, 0, 0);
    }

    CompensatingCounters snapshot(DefaultTierStatistics statistics) {
      return new CompensatingCounters(
        statistics.getHits() + hits,
        statistics.getMisses() + misses,
        statistics.getPuts() + puts,
        statistics.getRemovals() + removals,
        statistics.getEvictions() + evictions,
        statistics.getExpirations() + expirations
      );
    }
  }
}
