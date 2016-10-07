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
package org.ehcache.management.config;

import org.ehcache.management.providers.statistics.EhcacheStatisticsProvider;

import java.util.concurrent.TimeUnit;

public class EhcacheStatisticsProviderConfiguration extends DefaultStatisticsProviderConfiguration {

  /**
   *
   * @param averageWindowDuration Time window to calculate average.
   * @param averageWindowUnit TimeUnit of averageWindowDuration.  E.g. TimeUnit.MILLISECONDS, TimeUnit.MINUTES etc
   * @param historySize Number of statistics to keep in memory.  E.g. if set to 20 then the last 20 statistic values will be stored.
   * @param historyInterval The interval in which a sample is taken
   * @param historyIntervalUnit the time interval in which a sample is taken.
   * @param timeToDisable the time period in which a statistic is valid.  After this amount of time the statistic is removed.
   * @param timeToDisableUnit the TimeUnit associated with the timeToDisable parameter
   */
  public EhcacheStatisticsProviderConfiguration(long averageWindowDuration, TimeUnit averageWindowUnit, int historySize, long historyInterval, TimeUnit historyIntervalUnit, long timeToDisable, TimeUnit timeToDisableUnit) {
    super(EhcacheStatisticsProvider.class, averageWindowDuration, averageWindowUnit, historySize, historyInterval, historyIntervalUnit, timeToDisable, timeToDisableUnit);
  }

  public EhcacheStatisticsProviderConfiguration() {
    super(EhcacheStatisticsProvider.class);
  }

}
