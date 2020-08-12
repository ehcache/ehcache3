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
import org.terracotta.management.model.stats.ContextualStatistics;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.registry.StatisticQuery;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertThat;

public class StatsUtil {

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
  public static long getAndAssertExpectedValueFromCounter(String statName, Context context, ManagementRegistryService managementRegistry, long expectedResult) {

    StatisticQuery query = managementRegistry.withCapability("StatisticsCapability")
      .queryStatistics(singletonList(statName))
      .on(context)
      .build();

    ResultSet<ContextualStatistics> counters = query.execute();

    ContextualStatistics statisticsContext = counters.getResult(context);

    assertThat(counters.size(), Matchers.is(1));

    Number counter = statisticsContext.<Number>getLatestSampleValue(statName).get();
    long value = counter.longValue();

    assertThat(value, Matchers.is(expectedResult));

    return value;
  }

}
