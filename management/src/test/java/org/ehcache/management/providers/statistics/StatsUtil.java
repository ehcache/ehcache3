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

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertThat;

import org.ehcache.management.ManagementRegistryService;
import org.hamcrest.Matchers;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.stats.AbstractStatisticHistory;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.Statistic;
import org.terracotta.management.model.stats.StatisticHistory;
import org.terracotta.management.model.stats.history.AverageHistory;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.history.DurationHistory;
import org.terracotta.management.model.stats.history.RateHistory;
import org.terracotta.management.model.stats.history.RatioHistory;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;
import org.junit.Assert;
import java.util.Arrays;
import java.util.Map;

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
  public static long getAndAssertExpectedValueFromCounterHistory(String statName, Context context, ManagementRegistryService managementRegistry, long expectedResult) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
      .queryStatistics(singletonList(statName))
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
  public static void assertExpectedValueFromRatioHistory(String statName, Context context, ManagementRegistryService managementRegistry, double expectedResult) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
      .queryStatistics(singletonList(statName))
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
  }

  /*
  NOTE:  When using this method in other unit tests, make sure to declare a timeout as it is possible to get an infinite loop.
         This should only occur if the stats value is different from your minExpectedValue, which may happen if the stats calculations
         change, the stats value isn't accessible or if you enter the wrong minExpectedValue.
  */
  public static long assertExpectedValueFromDurationHistory(String statName, Context context, ManagementRegistryService managementRegistry, Long minExpectedValue) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(statName))
          .on(context)
          .build();

    Long value = null;
    do {
      ResultSet<ContextualStatistics> counters = query.execute();

      ContextualStatistics statisticsContext = counters.getResult(context);

      assertThat(counters.size(), Matchers.is(1));

      DurationHistory durationHistory = statisticsContext.getStatistic(DurationHistory.class, statName);

      if (durationHistory.getValue().length > 0) {
        int mostRecentIndex = durationHistory.getValue().length - 1;
        value = durationHistory.getValue()[mostRecentIndex].getValue();
      }

    }while(!Thread.currentThread().isInterrupted() && value == null);

    assertThat(value, Matchers.greaterThan(minExpectedValue));

    return value;
  }

  /*
  NOTE:  When using this method in other unit tests, make sure to declare a timeout as it is possible to get an infinite loop.
         This should only occur if the stats value is different from your minExpectedValue, which may happen if the stats calculations
         change, the stats value isn't accessible or if you enter the wrong minExpectedValue.
  */
  public static double assertExpectedValueFromAverageHistory(String statName, Context context, ManagementRegistryService managementRegistry) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(statName))
          .on(context)
          .build();

    double value = Double.NaN;
    do {
      ResultSet<ContextualStatistics> counters = query.execute();

      ContextualStatistics statisticsContext = counters.getResult(context);

      assertThat(counters.size(), Matchers.is(1));


      AverageHistory avgHistory = statisticsContext.getStatistic(AverageHistory.class, statName);//returns type DurationHistory but it was AverageHistory

      if (avgHistory.getValue().length > 0) {
        int mostRecentIndex = avgHistory.getValue().length - 1;
        value = avgHistory.getValue()[mostRecentIndex].getValue();
      }

    }while(!Thread.currentThread().isInterrupted() && Double.isNaN(value));

    assertThat(value, Matchers.greaterThan(0d));

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
        if (((StatisticHistory<?, ?>) entry.getValue()).getValue().length < 2) {
          noSample = true;
          break;
        }
      }
    } while (!Thread.currentThread().isInterrupted() && noSample);
  }

  /*
  NOTE:  When using this method in other unit tests, make sure to declare a timeout as it is possible to get an infinite loop.
         This should only occur if the stats value is different from your expectedResult, which may happen if the stats calculations
         change, the stats value isn't accessible or if you enter the wrong expectedResult.
  */
  public static double getAndAssertExpectedValueFromRateHistory(String statName, Context context, ManagementRegistryService managementRegistry, Double expectedResult) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(statName))
          .on(context)
          .build();

    Double value = 0d;
    do {
      ResultSet<ContextualStatistics> counters = query.execute();

      ContextualStatistics statisticsContext = counters.getResult(context);

      Assert.assertThat(counters.size(), Matchers.is(1));

      RateHistory rateHistory = statisticsContext.getStatistic(RateHistory.class, statName);

      if (rateHistory.getValue().length > 0) {
        int mostRecentIndex = rateHistory.getValue().length - 1;
        value = rateHistory.getValue()[mostRecentIndex].getValue();
      }

    }while(!Thread.currentThread().isInterrupted() && !value.equals(expectedResult));

    Assert.assertThat(value, Matchers.is(expectedResult));

    return value;
  }

  /*
  NOTE:  When using this method in other unit tests, make sure to declare a timeout as it is possible to get an infinite loop.
         This should only occur if the stats value is different from your minExpectedValue, which may happen if the stats calculations
         change, the stats value isn't accessible or if you enter the wrong minExpectedValue.
  */
  public static long getExpectedValueFromDurationHistory(String statName, Context context, ManagementRegistryService managementRegistry, Long minExpectedValue) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(statName))
          .on(context)
          .build();

    Long value = null;
    do {
      ResultSet<ContextualStatistics> counters = query.execute();

      ContextualStatistics statisticsContext = counters.getResult(context);

      Assert.assertThat(counters.size(), Matchers.is(1));

      DurationHistory durationHistory = statisticsContext.getStatistic(DurationHistory.class, statName);

      if (durationHistory.getValue().length > 0) {
        int mostRecentIndex = durationHistory.getValue().length - 1;
        value = durationHistory.getValue()[mostRecentIndex].getValue();
      }

    }while(!Thread.currentThread().isInterrupted() && value == null);

    Assert.assertThat(value, Matchers.greaterThan(minExpectedValue));

    return value;
  }

  /*
  NOTE:  When using this method in other unit tests, make sure to declare a timeout as it is possible to get an infinite loop.
         This should only occur if the stats value is different from your minExpectedValue, which may happen if the stats calculations
         change, the stats value isn't accessible or if you enter the wrong minExpectedValue.
  */
  public static double getExpectedValueFromAverageHistory(String statName, Context context, ManagementRegistryService managementRegistry, double minExpectedValue) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
          .queryStatistics(Arrays.asList(statName))
          .on(context)
          .build();

    double value = 0;
    do {
      ResultSet<ContextualStatistics> counters = query.execute();

      ContextualStatistics statisticsContext = counters.getResult(context);

      Assert.assertThat(counters.size(), Matchers.is(1));

      AverageHistory avgHistory = statisticsContext.getStatistic(AverageHistory.class, statName);

      if (avgHistory.getValue().length > 0) {
        int mostRecentIndex = avgHistory.getValue().length - 1;
        value = avgHistory.getValue()[mostRecentIndex].getValue();
      }

    }while(!Thread.currentThread().isInterrupted() && value <= minExpectedValue);

    Assert.assertThat(value, Matchers.greaterThan(minExpectedValue));

    return value;
  }
}
