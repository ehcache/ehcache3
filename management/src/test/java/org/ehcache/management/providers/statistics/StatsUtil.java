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

import java.util.Arrays;
import org.ehcache.management.ManagementRegistryService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.model.context.ContextContainer;
import org.terracotta.management.model.stats.AbstractStatisticHistory;
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.model.stats.history.CounterHistory;
import org.terracotta.management.model.stats.history.RatioHistory;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;

/**
 *
 *
 */
public class StatsUtil {

  public static boolean isHistoryReady(AbstractStatisticHistory counterHistory, Double defaultValue) {

    if(counterHistory.getValue().length > 0) {
      int mostRecentIndex = counterHistory.getValue().length - 1;
      if(defaultValue.equals(Double.POSITIVE_INFINITY)) {
        if((Double)counterHistory.getValue()[mostRecentIndex].getValue() < defaultValue ) {
          return true;
        }
      } else {
        if((Double)counterHistory.getValue()[mostRecentIndex].getValue() > defaultValue ) {
          return true;
        }
      }

    }
    return false;
  }

  public static boolean isHistoryReady(AbstractStatisticHistory counterHistory) {
    if(counterHistory.getValue().length > 0) {
      return true;
    }
    return false;
  }

  public static boolean isHistoryReady(AbstractStatisticHistory history, Long defaultValue) {
    if(history.getValue().length > 0) {
      int mostRecentIndex = history.getValue().length - 1;
      if((Long)history.getValue()[mostRecentIndex].getValue() > defaultValue) {
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

      Assert.assertThat(counters.size(), Matchers.is(1));

      CounterHistory counterHistory = statisticsContext.getStatistic(CounterHistory.class, statName);

      if (counterHistory.getValue().length > 0) {
        int mostRecentIndex = counterHistory.getValue().length - 1;
        value = counterHistory.getValue()[mostRecentIndex].getValue();
        System.out.println("statName: " + statName + " value: " + value + " expectedResult: " + expectedResult);
      }

    }while(value != expectedResult);

    Assert.assertThat(value, Matchers.is(expectedResult));

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

      Assert.assertThat(counters.size(), Matchers.is(1));

      RatioHistory ratioHistory = statisticsContext.getStatistic(RatioHistory.class, statName);

      if (ratioHistory.getValue().length > 0) {
        int mostRecentIndex = ratioHistory.getValue().length - 1;
        value = ratioHistory.getValue()[mostRecentIndex].getValue();
        System.out.println("statName: " + statName + " value: " + value + " expectedResult: " + expectedResult);
      }

    }while(value != expectedResult);

    Assert.assertThat(value, Matchers.is(expectedResult));

    return value;
  }
}
