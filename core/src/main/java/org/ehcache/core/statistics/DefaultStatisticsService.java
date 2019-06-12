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
import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.core.InternalCache;
import org.ehcache.core.events.CacheManagerListener;
import org.ehcache.core.spi.service.CacheManagerProviderService;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.InternalCacheManager;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.OptionalServiceDependencies;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.management.model.capabilities.descriptors.StatisticDescriptor;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.model.stats.StatisticRegistry;
import org.terracotta.management.model.stats.StatisticType;
import org.terracotta.statistics.MappedOperationStatistic;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.derived.OperationResultFilter;
import org.terracotta.statistics.derived.latency.DefaultLatencyHistogramStatistic;

import java.io.Serializable;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.ehcache.core.statistics.StatsUtils.findOperationStatisticOnChildren;
import static org.terracotta.statistics.StatisticBuilder.operation;

/**
 * Default implementation using the statistics calculated by the observers set on the caches.
 */
@OptionalServiceDependencies({"org.ehcache.core.spi.service.CacheManagerProviderService"})
public class DefaultStatisticsService implements StatisticsService, CacheManagerListener {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultStatisticsService.class);

  private final ConcurrentMap<String, DefaultCacheStatistics> cacheStatistics = new ConcurrentHashMap<>();
  private final ConcurrentMap<String, StatisticRegistry> statisticRegistries = new ConcurrentHashMap<>();

  private volatile InternalCacheManager cacheManager;
  private volatile boolean started = false;

  public CacheStatistics getCacheStatistics(String cacheName) {
    CacheStatistics stats = cacheStatistics.get(cacheName);
    if (stats == null) {
      throw new IllegalArgumentException("Unknown cache: " + cacheName);
    }
    return stats;
  }

  @Override
  public void registerWithParent(Object toAssociate, Object parent) {
    StatisticsManager.associate(toAssociate).withParent(parent);
  }

  @Override
  public <K, V, S extends Enum<S>, T extends Enum<T>> org.ehcache.core.statistics.OperationStatistic<T> registerStoreStatistics(Store<K, V> store, String targetName, int tierHeight, String tag, Map<T, Set<S>> translation, String statisticName) {

    Class<S> outcomeType = getOutcomeType(translation);

    // If the original stat doesn't exist, we do not need to translate it
    if (StatsUtils.hasOperationStat(store, outcomeType, targetName)) {

      MappedOperationStatistic<S, T> operationStatistic = new MappedOperationStatistic<>(store, translation, statisticName, tierHeight, targetName, tag);
      StatisticsManager.associate(operationStatistic).withParent(store);
      org.ehcache.core.statistics.OperationStatistic<T> stat = new DelegatedMappedOperationStatistics<>(operationStatistic);
      return stat;
    }
    return ZeroOperationStatistic.get();
  }

  /**
   * From the Map of translation, we extract one of the items to get the declaring class of the enum.
   *
   * @param translation translation map
   * @param <S> type of the outcome
   * @param <T> type of the possible translations
   * @return the outcome type
   */
  private static <S extends Enum<S>, T extends Enum<T>> Class<S> getOutcomeType(Map<T, Set<S>> translation) {
    Map.Entry<T, Set<S>> first = translation.entrySet().iterator().next();
    Class<S> outcomeType = first.getValue().iterator().next().getDeclaringClass();
    return outcomeType;
  }

  @Override
  public void deRegisterFromParent(Object toDeassociate, Object parent) {
    StatisticsManager.dissociate(toDeassociate).fromParent(parent);
  }

  @Override
  public void cleanForNode(Object node) {
    StatisticsManager.nodeFor(node).clean();
  }

  @Override
  public <K, V> void createCacheRegistry(String cacheName, Cache<K, V> cache, LongSupplier timeSource) {
    statisticRegistries.put(cacheName, new StatisticRegistry(cache, timeSource));
  }

  @Override
  public void registerCacheStatistics(String cacheName) {
    cacheStatistics.get(cacheName).getKnownStatistics().forEach(statisticRegistries.get(cacheName)::registerStatistic);
  }

  @Override
  public Collection<StatisticDescriptor> getCacheDescriptors(String cacheName) {
    return statisticRegistries.get(cacheName).getDescriptors();
  }

  @Override
  public <T extends Enum<T>, K, V> void registerDerivedStatistics(String cacheName, Cache<K, V> cache, String statName, T outcome, String derivedName, LatencyHistogramConfiguration configuration) {
    DefaultLatencyHistogramStatistic histogram = new DefaultLatencyHistogramStatistic(configuration.getPhi(), configuration.getBucketCount(), configuration.getWindow());

    @SuppressWarnings("unchecked")
    Class<T> outcomeClass = (Class<T>) outcome.getClass();
    OperationStatistic<T> stat = findOperationStatisticOnChildren(cache, outcomeClass, statName);
    stat.addDerivedStatistic(new OperationResultFilter<>(EnumSet.of(outcome), histogram));

    statisticRegistries.get(cacheName).registerStatistic(derivedName + "#50", histogram.medianStatistic());
    statisticRegistries.get(cacheName).registerStatistic(derivedName + "#95", histogram.percentileStatistic(0.95));
    statisticRegistries.get(cacheName).registerStatistic(derivedName + "#99", histogram.percentileStatistic(0.99));
    statisticRegistries.get(cacheName).registerStatistic(derivedName + "#100", histogram.maximumStatistic());
  }

  @Override
  public Map<String, Statistic<? extends Serializable>> collectStatistics(String cacheName, Collection<String> statisticNames, long since) {
    return StatisticRegistry.collect(statisticRegistries.get(cacheName), statisticNames, since);
  }

  @Override
  public <T extends Serializable> void registerStatistic(Object context, String name, StatisticType type, Set<String> tags, Supplier<T> valueSupplier) {
    StatisticsManager.createPassThroughStatistic(context, name, tags, StatisticType.convert(type), valueSupplier);
  }

  @Override
  public <T extends Enum<T>> OperationObserver<T> createOperationStatistics(String name, Class<T> outcome, String tag, Object context) {
    return new DelegatingOperationObserver<>(operation(outcome).named(name).of(context).tag(tag).build());
  }

  public boolean isStarted() {
    return started;
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    LOGGER.debug("Starting service");

    CacheManagerProviderService cacheManagerProviderService = serviceProvider.getService(CacheManagerProviderService.class);
    if (cacheManagerProviderService != null) {
      cacheManager = cacheManagerProviderService.getCacheManager();
      cacheManager.registerListener(this);
    }

    started = true;
  }

  @Override
  public void stop() {
    LOGGER.debug("Stopping service");
    cacheManager.deregisterListener(this);
    cacheStatistics.clear();
    started = false;
  }

  @Override
  public void stateTransition(Status from, Status to) {
    LOGGER.debug("Moving from " + from + " to " + to);
    switch (to) {
      case AVAILABLE:
        registerAllCaches();
        break;
      case UNINITIALIZED:
        cacheManager.deregisterListener(this);
        cacheStatistics.clear();
        break;
      case MAINTENANCE:
        throw new IllegalStateException("Should not be started in maintenance mode");
      default:
        throw new AssertionError("Unsupported state: " + to);
    }
  }

  private void registerAllCaches() {
    for (Map.Entry<String, CacheConfiguration<?, ?>> entry : cacheManager.getRuntimeConfiguration().getCacheConfigurations().entrySet()) {
      String alias = entry.getKey();
      CacheConfiguration<?, ?> configuration = entry.getValue();
      Cache<?, ?> cache = cacheManager.getCache(alias, configuration.getKeyType(), configuration.getValueType());
      cacheAdded(alias, cache);
    }
  }

  @Override
  public void cacheAdded(String alias, Cache<?, ?> cache) {
    LOGGER.debug("Cache added " + alias);
    cacheStatistics.put(alias, new DefaultCacheStatistics((InternalCache<?, ?>) cache));
  }

  @Override
  public void cacheRemoved(String alias, Cache<?, ?> cache) {
    LOGGER.debug("Cache removed " + alias);
    cacheStatistics.remove(alias);
  }

}
