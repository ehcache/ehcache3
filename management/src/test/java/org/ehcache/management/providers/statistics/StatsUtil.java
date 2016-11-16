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

import org.ehcache.management.ManagementRegistryService;
import org.hamcrest.Matchers;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.stats.AbstractStatisticHistory;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.model.stats.StatisticHistory;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.history.RatioHistory;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;

import java.util.Arrays;
import java.util.Map;

import static org.junit.Assert.assertThat;

public class StatsUtil {

  public static boolean isHistoryReady(AbstractStatisticHistory counterHistory) {
    if (counterHistory.getValue().length > 0) {
      return true;
    }
    return false;
  }

  public static boolean isHistoryReady(AbstractStatisticHistory history, Long defaultValue) {
    if (history.getValue().length > 0) {
      int mostRecentIndex = history.getValue().length - 1;
      if ((Long) history.getValue()[mostRecentIndex].getValue() > defaultValue) {
        return true;
      }
    }
    return false;
  }

  public static Context createContext(ManagementRegistryService managementRegistry) {
    ContextContainer cacheManagerCtx = managementRegistry.getContextContainer();
    ContextContainer firstCacheCtx = cacheManagerCtx.getSubContexts().iterator().next();
    return Context.empty()
      .with(cacheManagerCtx.getName(), cacheManagerCtx.getValue())
      .with(firstCacheCtx.getName(), firstCacheCtx.getValue());
  }

  /*
  NOTE:  When using this method in other unit tests, make sure to declare a timeout as it is possible to get an infinite loop.
         This should only occur if the stats value is different from your expectedResult, which may happen if the stats calculations
         change, the stats value isn't accessible or if you enter the wrong expectedResult.
  */
  public static long getExpectedValueFromCounterHistory(String statName, Context context, ManagementRegistryService managementRegistry, long expectedResult) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
      .queryStatistics(Arrays.asList(statName))
      .on(context)
      .build();

    long value = 0;
    do {
      ResultSet<ContextualStatistics> counters = query.execute();

      ContextualStatistics statisticsContext = counters.getResult(context);

      assertThat(counters.size(), Matchers.is(1));

      CounterHistory counterHistory = statisticsContext.getStatistic(CounterHistory.class, statName);

      if (counterHistory.getValue().length > 0) {
        int mostRecentIndex = counterHistory.getValue().length - 1;
        value = counterHistory.getValue()[mostRecentIndex].getValue();
      }

    } while (!Thread.currentThread().isInterrupted() && value != expectedResult);

    assertThat(value, Matchers.is(expectedResult));

    return value;
  }

  /*
  NOTE:  When using this method in other unit tests, make sure to declare a timeout as it is possible to get an infinite loop.
         This should only occur if the stats value is different from your expectedResult, which may happen if the stats calculations
         change, the stats value isn't accessible or if you enter the wrong expectedResult.
  */
  public static double getExpectedValueFromRatioHistory(String statName, Context context, ManagementRegistryService managementRegistry, double expectedResult) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
      .queryStatistics(Arrays.asList(statName))
      .on(context)
      .build();

    double value = 0;
    do {
      ResultSet<ContextualStatistics> counters = query.execute();

      ContextualStatistics statisticsContext = counters.getResult(context);

      assertThat(counters.size(), Matchers.is(1));

      RatioHistory ratioHistory = statisticsContext.getStatistic(RatioHistory.class, statName);

      if (ratioHistory.getValue().length > 0) {
        int mostRecentIndex = ratioHistory.getValue().length - 1;
        value = ratioHistory.getValue()[mostRecentIndex].getValue();
      }
    } while (!Thread.currentThread().isInterrupted() && value != expectedResult);

    assertThat(value, Matchers.is(expectedResult));

    return value;
  }

  // When testing ratios, we need to wait for the first computation (we do not have any choice) to happen because ratio depends on 2 other sampled statistics.
  // If you do not wait, then you'll always get some NaN because the hits will be done within the 1st second, and the hits won't be done in the right "window".
  // A ratio is computed by dividing a rate with another rate. See CompoundOperationImpl.ratioOf().
  // And a rate is computed with values aggregated into a EventRateSimpleMovingAverage.
  // The call to EventRateSimpleMovingAverage.rateUsingSeconds() will return 0 during the fist second (until first computation did happen).
  // So the hits must be after the first second so that values get accumulated into the partitions of EventRateSimpleMovingAverage.

  // Also, we have to take in consideration that in clustered, there is a collector that is scheduled at 75% of the TTD to collect and send stats.
  // So the delay can be greater than just the duration of the first sampling.
  public static void triggerStatComputation(ManagementRegistryService managementRegistry, Context context, String... statNames) {
    boolean noSample;
    do {
      noSample = false;
      Map<String, Statistic<?, ?>> statistics = managementRegistry.withCapability("StatisticsCapability")
        .queryStatistics(Arrays.asList(statNames))
        .on(context)
        .build()
        .execute()
        .getSingleResult()
        .getStatistics();

      for (Map.Entry<String, Statistic<?, ?>> entry : statistics.entrySet()) {
        if (((StatisticHistory<?, ?>) entry.getValue()).getValue().length == 0) {
          noSample = true;
          break;
        }
      }
    } while (!Thread.currentThread().isInterrupted() && noSample);
  }

}
