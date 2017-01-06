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
package org.ehcache.management.providers.statistics;

import org.ehcache.core.statistics.CacheOperationOutcomes;
import org.ehcache.core.statistics.TierOperationOutcomes;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.ExposedCacheBinding;
import org.terracotta.context.extended.OperationStatisticDescriptor;
import org.terracotta.context.extended.StatisticsRegistry;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.registry.collect.StatisticsRegistryMetadata;

import java.util.Collection;
import java.util.EnumSet;
import java.util.concurrent.ScheduledExecutorService;

import static java.util.Collections.singleton;
import static java.util.EnumSet.allOf;
import static java.util.EnumSet.of;
import static org.terracotta.context.extended.ValueStatisticDescriptor.descriptor;

class StandardEhcacheStatistics extends ExposedCacheBinding {

  private final StatisticsRegistry statisticsRegistry;
  private final StatisticsRegistryMetadata statisticsRegistryMetadata;

  StandardEhcacheStatistics(ManagementRegistryServiceConfiguration registryConfiguration, CacheBinding cacheBinding, StatisticsProviderConfiguration statisticsProviderConfiguration, ScheduledExecutorService executor) {
    super(registryConfiguration, cacheBinding);
    this.statisticsRegistry = new StatisticsRegistry(cacheBinding.getCache(), executor, statisticsProviderConfiguration.averageWindowDuration(),
        statisticsProviderConfiguration.averageWindowUnit(), statisticsProviderConfiguration.historySize(), statisticsProviderConfiguration.historyInterval(), statisticsProviderConfiguration.historyIntervalUnit(),
        statisticsProviderConfiguration.timeToDisable(), statisticsProviderConfiguration.timeToDisableUnit());

    this.statisticsRegistryMetadata = new StatisticsRegistryMetadata(statisticsRegistry);

    EnumSet<CacheOperationOutcomes.GetOutcome> hit = of(CacheOperationOutcomes.GetOutcome.HIT);
    EnumSet<CacheOperationOutcomes.GetOutcome> miss = of(CacheOperationOutcomes.GetOutcome.MISS);
    OperationStatisticDescriptor<CacheOperationOutcomes.GetOutcome> getCacheStatisticDescriptor = OperationStatisticDescriptor.descriptor("get", singleton("cache"), CacheOperationOutcomes.GetOutcome.class);

    statisticsRegistry.registerCompoundOperations("Cache:Hit", getCacheStatisticDescriptor, hit);
    statisticsRegistry.registerCompoundOperations("Cache:Miss", getCacheStatisticDescriptor, miss);
    statisticsRegistry.registerCompoundOperations("Cache:Clear", OperationStatisticDescriptor.descriptor("clear", singleton("cache"),CacheOperationOutcomes.ClearOutcome.class), allOf(CacheOperationOutcomes.ClearOutcome.class));
    statisticsRegistry.registerRatios("Cache:HitRatio", getCacheStatisticDescriptor, hit, allOf(CacheOperationOutcomes.GetOutcome.class));
    statisticsRegistry.registerRatios("Cache:MissRatio", getCacheStatisticDescriptor, miss, allOf(CacheOperationOutcomes.GetOutcome.class));

    Class<TierOperationOutcomes.GetOutcome> tierOperationGetOucomeClass = TierOperationOutcomes.GetOutcome.class;
    OperationStatisticDescriptor<TierOperationOutcomes.GetOutcome> getTierStatisticDescriptor = OperationStatisticDescriptor.descriptor("get", singleton("tier"), tierOperationGetOucomeClass);

    statisticsRegistry.registerCompoundOperations("Hit", getTierStatisticDescriptor, of(TierOperationOutcomes.GetOutcome.HIT));
    statisticsRegistry.registerCompoundOperations("Miss", getTierStatisticDescriptor, of(TierOperationOutcomes.GetOutcome.MISS));
    statisticsRegistry.registerCompoundOperations("Eviction",
        OperationStatisticDescriptor.descriptor("eviction", singleton("tier"),
        TierOperationOutcomes.EvictionOutcome.class),
        allOf(TierOperationOutcomes.EvictionOutcome.class));
    statisticsRegistry.registerRatios("HitRatio", getTierStatisticDescriptor, of(TierOperationOutcomes.GetOutcome.HIT),  allOf(tierOperationGetOucomeClass));
    statisticsRegistry.registerRatios("MissRatio", getTierStatisticDescriptor, of(TierOperationOutcomes.GetOutcome.MISS),  allOf(tierOperationGetOucomeClass));
    statisticsRegistry.registerCounter("MappingCount", descriptor("mappings", singleton("tier")));
    statisticsRegistry.registerCounter("MaxMappingCount", descriptor("maxMappings", singleton("tier")));
    statisticsRegistry.registerSize("AllocatedByteSize", descriptor("allocatedMemory", singleton("tier")));
    statisticsRegistry.registerSize("OccupiedByteSize", descriptor("occupiedMemory", singleton("tier")));

    //new stats architecture changes.  The above registrations will be removed once the following task is completed: https://github.com/ehcache/ehcache3/issues/1684
    registerStatCacheOutcomes();
    registerStatTierOutcomes();
  }

  private void registerStatCacheOutcomes() {
    //TODO Statistics library needs StatisticsRegistry.registerOperation(...) method.  Once created replace all calls to registerCompoundOperations(...) with it.
    //use the following format to define a cache operation: Cache:operation:outcome

    //register Cache outcomes for Hit stat calculations
    statisticsRegistry.registerCompoundOperations("Cache:Get:HitNoLoader", OperationStatisticDescriptor.descriptor("get", singleton("cache"), CacheOperationOutcomes.GetOutcome.class), of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER));
    statisticsRegistry.registerCompoundOperations("Cache:Get:HitWithLoader", OperationStatisticDescriptor.descriptor("get", singleton("cache"), CacheOperationOutcomes.GetOutcome.class), of(CacheOperationOutcomes.GetOutcome.HIT_WITH_LOADER));
    statisticsRegistry.registerCompoundOperations("Cache:Replace:Hit", OperationStatisticDescriptor.descriptor("replace", singleton("cache"), CacheOperationOutcomes.ReplaceOutcome.class), of(CacheOperationOutcomes.ReplaceOutcome.HIT));
    statisticsRegistry.registerCompoundOperations("Cache:Replace:MissPresent", OperationStatisticDescriptor.descriptor("replace", singleton("cache"), CacheOperationOutcomes.ReplaceOutcome.class), of(CacheOperationOutcomes.ReplaceOutcome.MISS_PRESENT));
    statisticsRegistry.registerCompoundOperations("Cache:ConditionalRemove:Success", OperationStatisticDescriptor.descriptor("conditionalRemove", singleton("cache"), CacheOperationOutcomes.ConditionalRemoveOutcome.class), of(CacheOperationOutcomes.ConditionalRemoveOutcome.SUCCESS));
    statisticsRegistry.registerCompoundOperations("Cache:ConditionalRemove:FailureKeyPresent", OperationStatisticDescriptor.descriptor("conditionalRemove", singleton("cache"), CacheOperationOutcomes.ConditionalRemoveOutcome.class), of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_PRESENT));
    //TODO register BulkOps outcomes
    //statisticsRegistry.registerCompoundOperations("Cache:BulkOps:GetAllHits", OperationStatisticDescriptor.descriptor("getAll", singleton("cache"), BulkOps.class), of(BulkOps.GET_ALL_HITS));

    //register Cache outcomes for Miss stat calculations
    statisticsRegistry.registerCompoundOperations("Cache:Get:MissNoLoader", OperationStatisticDescriptor.descriptor("get", singleton("cache"), CacheOperationOutcomes.GetOutcome.class), of(CacheOperationOutcomes.GetOutcome.MISS_NO_LOADER));
    statisticsRegistry.registerCompoundOperations("Cache:Get:MissWithLoader", OperationStatisticDescriptor.descriptor("get", singleton("cache"), CacheOperationOutcomes.GetOutcome.class), of(CacheOperationOutcomes.GetOutcome.MISS_WITH_LOADER));
    statisticsRegistry.registerCompoundOperations("Cache:PutIfAbsent:Put", OperationStatisticDescriptor.descriptor("putIfAbsent", singleton("cache"), CacheOperationOutcomes.PutIfAbsentOutcome.class), of(CacheOperationOutcomes.PutIfAbsentOutcome.PUT));
    statisticsRegistry.registerCompoundOperations("Cache:Replace:MissNotPresent", OperationStatisticDescriptor.descriptor("replace", singleton("cache"), CacheOperationOutcomes.ReplaceOutcome.class), of(CacheOperationOutcomes.ReplaceOutcome.MISS_NOT_PRESENT));
    statisticsRegistry.registerCompoundOperations("Cache:ConditionalRemove:FailureKeyMissing", OperationStatisticDescriptor.descriptor("conditionalRemove", singleton("cache"), CacheOperationOutcomes.ConditionalRemoveOutcome.class), of(CacheOperationOutcomes.ConditionalRemoveOutcome.FAILURE_KEY_MISSING));
    //TODO register BulkOps outcomes
    //statisticsRegistry.registerCompoundOperations("Cache:BulkOps:GetAllMiss", OperationStatisticDescriptor.descriptor("getAll", singleton("cache"), BulkOps.class), of(BulkOps.GET_ALL_MISS));

    //register Cache outcomes for Remove stat calculations
    statisticsRegistry.registerCompoundOperations("Cache:Remove:Success", OperationStatisticDescriptor.descriptor("remove", singleton("cache"), CacheOperationOutcomes.RemoveOutcome.class), of(CacheOperationOutcomes.RemoveOutcome.SUCCESS));
    //"Cache:ConditionalRemove:Success" already registered
    //TODO register BulkOps outcomes
    //statisticsRegistry.registerCompoundOperations("Cache:BulkOps:RemoveAll", OperationStatisticDescriptor.descriptor("removeAll", singleton("cache"), BulkOps.class), of(BulkOps.REMOVE_ALL));

    //register Cache outcomes for Replace stat calculations
    //"Cache:Replace:Hit" already registered
    statisticsRegistry.registerCompoundOperations("Cache:Put:Updated", OperationStatisticDescriptor.descriptor("put", singleton("cache"), CacheOperationOutcomes.PutOutcome.class), of(CacheOperationOutcomes.PutOutcome.UPDATED));

    //register Cache outcomes for Put stat calculations
    statisticsRegistry.registerCompoundOperations("Cache:Put:Put", OperationStatisticDescriptor.descriptor("put", singleton("cache"), CacheOperationOutcomes.PutOutcome.class), of(CacheOperationOutcomes.PutOutcome.PUT));
    //"Cache:Put:Updated" already registered
    //"Cache:PutIfAbsent:Put" already registered
    //"Cache:Replace:Hit" already registered
    //TODO register BulkOps outcomes
    //statisticsRegistry.registerCompoundOperations("Cache:BulkOps:PutAll", OperationStatisticDescriptor.descriptor("putAll", singleton("cache"), CacheOperationOutcomes.PutAllOutcome.class), of(CacheOperationOutcomes.PutAllOutcome.SUCCESS));
  }

  private void registerStatTierOutcomes() {

    //use the following format to define a tier operation: operation:outcome
    //The tier will be pre-appended

    //TODO register Tier outcomes for Hit stat calculations

    //TODO register Tier outcomes for Miss stat calculations

    //TODO register Tier outcomes for Remove stat calculations

    //TODO register Tier outcomes for Replace stat calculations

    //TODO register Tier outcomes for Put stat calculations

    //register Tier outcomes for Eviction.  Evictions only occur in the authoritative tier and there is no Cache level eviction outcome.
    statisticsRegistry.registerCompoundOperations("Eviction:Success", OperationStatisticDescriptor.descriptor("eviction", singleton("tier"), TierOperationOutcomes.EvictionOutcome.class), of(TierOperationOutcomes.EvictionOutcome.SUCCESS));

    //register Tier outcomes for Expiration
    statisticsRegistry.registerCompoundOperations("Expiration:Success", OperationStatisticDescriptor.descriptor("expiration", singleton("tier"), TierOperationOutcomes.ExpirationOutcome.class), of(TierOperationOutcomes.ExpirationOutcome.SUCCESS));

  }

  Statistic<?, ?> queryStatistic(String fullStatisticName, long since) {
    return statisticsRegistryMetadata.queryStatistic(fullStatisticName, since);
  }

  @Override
  public Collection<? extends StatisticDescriptor> getDescriptors() {
    return statisticsRegistryMetadata.getDescriptors();
  }

  void dispose() {
    statisticsRegistry.clearRegistrations();
  }


}
