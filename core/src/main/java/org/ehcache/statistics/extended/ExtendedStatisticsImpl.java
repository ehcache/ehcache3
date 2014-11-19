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

package org.ehcache.statistics.extended;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.terracotta.context.query.Matchers.attributes;
import static org.terracotta.context.query.Matchers.context;
import static org.terracotta.context.query.Matchers.hasAttribute;
import static org.terracotta.context.query.Matchers.identifier;
import static org.terracotta.context.query.Matchers.subclassOf;
import static org.terracotta.context.query.QueryBuilder.queryBuilder;

import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.ehcache.statistics.CacheOperationOutcomes;
import org.ehcache.statistics.CacheOperationOutcomes.EvictionOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.GetOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.PutIfAbsentOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.RemoveOutcome;
import org.ehcache.statistics.CacheOperationOutcomes.ReplaceOutcome;
import org.ehcache.statistics.StatisticsGateway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.terracotta.context.TreeNode;
import org.terracotta.context.query.Matcher;
import org.terracotta.context.query.Matchers;
import org.terracotta.context.query.Query;
import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.StatisticsManager;
import org.terracotta.statistics.Time;

/**
 * @author cschanck
 * @author Hung Huynh
 *
 */
public class ExtendedStatisticsImpl implements ExtendedStatistics {

  /** The Constant LOGGER. */
  private static final Logger                                                  LOGGER             = LoggerFactory
                                                                                                      .getLogger(ExtendedStatisticsImpl.class);
  /** The standard operations. */
  private final ConcurrentMap<StandardOperationStatistic, Operation<?>>        standardOperations = new ConcurrentHashMap<StandardOperationStatistic, Operation<?>>();

  /** The custom operations. */
  private final ConcurrentMap<OperationStatistic<?>, CompoundOperationImpl<?>> customOperations   = new ConcurrentHashMap<OperationStatistic<?>, CompoundOperationImpl<?>>();

  /** The manager. */
  private final StatisticsManager                                              manager;

  /** The executor. */
  private final ScheduledExecutorService                                       executor;

  /** The disable task. */
  private final Runnable                                                       disableTask;

  /** The time to disable. */
  private long                                                                 timeToDisable;

  /** The time to disable unit. */
  private TimeUnit                                                             timeToDisableUnit;

  /** The disable status. */
  private ScheduledFuture                                                      disableStatus;

  /** The all cache get. */
  private final Result                                                         allCacheGet;

  /** The all cache miss. */
  private final Result                                                         allCacheMiss;

  /** The all cache put. */
  private final Result                                                         allCachePut;

  /** The all cache remove. */
  private final Result                                                         allCacheRemove;

  private final Result                                                         getWithLoader;

  private final Result                                                         getNoLoader;
  
  private final Result                                                         cacheLoader;

  /** The cache hit ratio. */
  private Statistic<Double>                                                    cacheHitRatio;

  private final int                                                            defaultHistorySize;

  private final long                                                           defaultIntervalSeconds;

  private final long                                                           defaultSearchIntervalSeconds;

  /**
   * Instantiates a new extended statistics impl.
   * 
   * @param manager
   *          the manager
   * @param executor
   *          the executor
   * @param timeToDisable
   *          the time to disable
   * @param unit
   *          the unit
   */
  @SuppressWarnings("unchecked")
  public ExtendedStatisticsImpl(StatisticsManager manager, ScheduledExecutorService executor,
      long timeToDisable, TimeUnit unit, int defaultHistorySize, long defaultIntervalSeconds,
      long defaultSearchIntervalSeconds) {
    this.manager = manager;
    this.executor = executor;
    this.timeToDisable = timeToDisable;
    this.timeToDisableUnit = unit;
    this.defaultHistorySize = defaultHistorySize;
    this.defaultIntervalSeconds = defaultIntervalSeconds;
    this.defaultSearchIntervalSeconds = defaultSearchIntervalSeconds;
    this.disableTask = createDisableTask();

    this.disableStatus = this.executor.scheduleAtFixedRate(disableTask, timeToDisable,
        timeToDisable, unit);

    findStandardOperationStatistics();

    // well known compound results.
    this.allCacheGet = get().compound(ALL_CACHE_GET_OUTCOMES);
    this.allCacheMiss = get().compound(ALL_CACHE_MISS_OUTCOMES);
    this.allCachePut = put().compound(ALL_CACHE_PUT_OUTCOMES);
    this.allCacheRemove = remove().compound(ALL_CACHE_REMOVE_OUTCOMES);
    this.getWithLoader = get().compound(GET_WITH_LOADER_OUTCOMES);
    this.getNoLoader = get().compound(GET_NO_LOADER_OUTCOMES);
    this.cacheLoader = ((Operation<CacheOperationOutcomes.CacheLoaderOutcome>) getStandardOperation(StandardOperationStatistic.CACHE_LOADER))
        .compound(ALL_CACHE_LOADER_OUTCOMES);

    this.cacheHitRatio = get().ratioOf(EnumSet.of(CacheOperationOutcomes.GetOutcome.HIT_NO_LOADER),
        EnumSet.allOf(CacheOperationOutcomes.GetOutcome.class));
  }

  private Runnable createDisableTask() {
    return new Runnable() {
      @Override
      public void run() {
        long expireThreshold = Time.absoluteTime() - timeToDisableUnit.toMillis(timeToDisable);
        for (Operation<?> o : standardOperations.values()) {
          if (o instanceof CompoundOperationImpl<?>) {
            ((CompoundOperationImpl<?>) o).expire(expireThreshold);
          }
        }
        for (Iterator<CompoundOperationImpl<?>> it = customOperations.values().iterator(); it
            .hasNext();) {
          if (it.next().expire(expireThreshold)) {
            it.remove();
          }
        }
      }
    };
  }

  /**
   * Find standard operation statistics.
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private void findStandardOperationStatistics() {
    for (final StandardOperationStatistic t : StandardOperationStatistic.values()) {
      OperationStatistic statistic = findOperationStatistic(manager, t);
      if (statistic == null) {
        if (t.required()) {
          throw new IllegalStateException("Required statistic " + t + " not found");
        } else {
          LOGGER.debug("Mocking Operation Statistic: {}", t);
          standardOperations.put(t, NullCompoundOperation.instance(t.type()));
        }
      } else {
        standardOperations.put(t,
            new CompoundOperationImpl(statistic, t.type(),
                StatisticsGateway.DEFAULT_WINDOW_SIZE_SECS, SECONDS, executor, defaultHistorySize,
                t.isSearch() ? defaultSearchIntervalSeconds : defaultIntervalSeconds, SECONDS));
      }
    }
  }

  @Override
  public synchronized void setTimeToDisable(long time, TimeUnit unit) {
    timeToDisable = time;
    timeToDisableUnit = unit;
    if (disableStatus != null) {
      disableStatus.cancel(false);
      disableStatus = executor.scheduleAtFixedRate(disableTask, timeToDisable, timeToDisable,
          timeToDisableUnit);
    }
  }

  @Override
  public synchronized void setAlwaysOn(boolean enabled) {
    if (enabled) {
      if (disableStatus != null) {
        disableStatus.cancel(false);
        disableStatus = null;
      }
      for (Operation<?> o : standardOperations.values()) {
        o.setAlwaysOn(true);
      }
    } else {
      if (disableStatus == null) {
        disableStatus = executor.scheduleAtFixedRate(disableTask, 0, timeToDisable,
            timeToDisableUnit);
      }
      for (Operation<?> o : standardOperations.values()) {
        o.setAlwaysOn(false);
      }
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public Operation<GetOutcome> get() {
    return (Operation<GetOutcome>) getStandardOperation(StandardOperationStatistic.CACHE_GET);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Operation<PutOutcome> put() {
    return (Operation<PutOutcome>) getStandardOperation(StandardOperationStatistic.CACHE_PUT);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Operation<PutIfAbsentOutcome> putIfAbsent() {
    return (Operation<PutIfAbsentOutcome>) getStandardOperation(StandardOperationStatistic.CACHE_PUT_IF_ABSENT);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Operation<ReplaceOutcome> replace() {
    return (Operation<ReplaceOutcome>) getStandardOperation(StandardOperationStatistic.CACHE_REPLACE);
  }    

  @SuppressWarnings("unchecked")
  @Override
  public Operation<RemoveOutcome> remove() {
    return (Operation<RemoveOutcome>) getStandardOperation(StandardOperationStatistic.CACHE_REMOVE);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Operation<EvictionOutcome> eviction() {
    return (Operation<CacheOperationOutcomes.EvictionOutcome>) getStandardOperation(StandardOperationStatistic.EVICTION);
  }

  @Override
  public Statistic<Double> cacheHitRatio() {
    return cacheHitRatio;
  }

  @Override
  public Result allGet() {
    return allCacheGet;
  }

  @Override
  public Result allMiss() {
    return allCacheMiss;
  }

  @Override
  public Result allPut() {
    return allCachePut;
  }

  @Override
  public Result allRemove() {
    return allCacheRemove;
  }

  @Override
  public Result getWithLoader() {
    return getWithLoader;
  }

  @Override
  public Result getNoLoader() {
    return getNoLoader;
  }

  @Override
  public Result cacheLoader() {
    return cacheLoader;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends Enum<T>> Set<Operation<T>> operations(Class<T> outcome, String name,
      String... tags) {
    Set<OperationStatistic<T>> sources = findOperationStatistic(manager, queryBuilder()
        .descendants().build(), outcome, name, new HashSet<String>(Arrays.asList(tags)));
    if (sources.isEmpty()) {
      return Collections.emptySet();
    } else {
      Set<Operation<T>> operations = new HashSet<Operation<T>>();
      for (OperationStatistic<T> source : sources) {
        CompoundOperationImpl<T> operation = (CompoundOperationImpl<T>) customOperations
            .get(source);
        if (operation == null) {
          operation = new CompoundOperationImpl<T>(source, source.type(), 1, SECONDS, executor, 0,
              1, SECONDS);
          CompoundOperationImpl<T> racer = (CompoundOperationImpl<T>) customOperations.putIfAbsent(
              source, operation);
          if (racer != null) {
            operation = racer;
          }
        }
        operations.add(operation);
      }
      return operations;
    }
  }

  /**
   * Gets the standard operation.
   * 
   * @param statistic
   *          the statistic
   * @return the standard operation
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private Operation<?> getStandardOperation(StandardOperationStatistic statistic) {
    Operation<?> operation = standardOperations.get(statistic);
    if (operation instanceof NullCompoundOperation<?>) {
      OperationStatistic<?> discovered = findOperationStatistic(manager, statistic);
      if (discovered == null) {
        return operation;
      } else {
        Operation<?> newOperation = new CompoundOperationImpl(discovered, statistic.type(),
            StatisticsGateway.DEFAULT_WINDOW_SIZE_SECS, SECONDS, executor, defaultHistorySize,
            statistic.isSearch() ? defaultSearchIntervalSeconds : defaultIntervalSeconds, SECONDS);
        if (standardOperations.replace(statistic, operation, newOperation)) {
          return newOperation;
        } else {
          return standardOperations.get(statistic);
        }
      }
    } else {
      return operation;
    }
  }

  /**
   * Find operation statistic.
   * 
   * @param manager
   *          the manager
   * @param statistic
   *          the statistic
   * @return the operation statistic
   */
  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static OperationStatistic findOperationStatistic(StatisticsManager manager,
      StandardOperationStatistic statistic) {
    Set<OperationStatistic<? extends Enum>> results = findOperationStatistic(manager,
        statistic.context(), statistic.type(), statistic.operationName(), statistic.tags());
    switch (results.size()) {
    case 0:
      return null;
    case 1:
      return results.iterator().next();
    default:
      throw new IllegalStateException("Duplicate statistics found for " + statistic);
    }
  }

  /**
   * Find operation statistic.
   * 
   * @param <T>
   *          the generic type
   * @param manager
   *          the manager
   * @param contextQuery
   *          the context query
   * @param type
   *          the type
   * @param name
   *          the name
   * @param tags
   *          the tags
   * @return the sets the
   */
  @SuppressWarnings("unchecked")
  private static <T extends Enum<T>> Set<OperationStatistic<T>> findOperationStatistic(
      StatisticsManager manager, Query contextQuery, Class<T> type, String name,
      final Set<String> tags) {
    Set<TreeNode> operationStatisticNodes = manager.query(queryBuilder().chain(contextQuery)
        .children().filter(context(identifier(subclassOf(OperationStatistic.class)))).build());
    Set<TreeNode> result = queryBuilder()
        .filter(
            context(attributes(Matchers.<Map<String, Object>> allOf(hasAttribute("type", type),
                hasAttribute("name", name), hasAttribute("tags", new Matcher<Set<String>>() {
                  @Override
                  protected boolean matchesSafely(Set<String> object) {
                    return object.containsAll(tags);
                  }
                }))))).build().execute(operationStatisticNodes);

    if (result.isEmpty()) {
      return Collections.emptySet();
    } else {
      Set<OperationStatistic<T>> statistics = new HashSet<OperationStatistic<T>>();
      for (TreeNode node : result) {
        statistics.add((OperationStatistic<T>) node.getContext().attributes().get("this"));
      }
      return statistics;
    }
  }
}
