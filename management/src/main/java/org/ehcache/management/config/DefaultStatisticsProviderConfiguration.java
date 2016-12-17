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

import org.terracotta.management.model.Objects;
import org.terracotta.management.registry.ManagementProvider;
import org.terracotta.management.registry.collect.StatisticConfiguration;

import java.util.concurrent.TimeUnit;

public class DefaultStatisticsProviderConfiguration extends StatisticConfiguration implements StatisticsProviderConfiguration {

  private static final long serialVersionUID = 1L;

  private final Class<? extends ManagementProvider> provider;

  public DefaultStatisticsProviderConfiguration(Class<? extends ManagementProvider> provider, long averageWindowDuration, TimeUnit averageWindowUnit, int historySize, long historyInterval, TimeUnit historyIntervalUnit, long timeToDisable, TimeUnit timeToDisableUnit) {
    super(averageWindowDuration, averageWindowUnit, historySize, historyInterval, historyIntervalUnit, timeToDisable, timeToDisableUnit);
    this.provider = Objects.requireNonNull(provider);
  }

  public DefaultStatisticsProviderConfiguration(Class<? extends ManagementProvider> provider) {
    this.provider = Objects.requireNonNull(provider);
  }

  @Override
  public Class<? extends ManagementProvider> getStatisticsProviderType() {
    return provider;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    DefaultStatisticsProviderConfiguration that = (DefaultStatisticsProviderConfiguration) o;
    return provider.equals(that.provider);
  }

  @Override
  public int hashCode() {
    int result = super.hashCode();
    result = 31 * result + provider.hashCode();
    return result;
  }

  @Override
  public DefaultStatisticsProviderConfiguration setAverageWindowDuration(long averageWindowDuration, TimeUnit averageWindowUnit) {
    super.setAverageWindowDuration(averageWindowDuration, averageWindowUnit);
    return this;
  }

  @Override
  public DefaultStatisticsProviderConfiguration setHistoryInterval(long historyInterval, TimeUnit historyIntervalUnit) {
    super.setHistoryInterval(historyInterval, historyIntervalUnit);
    return this;
  }

  @Override
  public DefaultStatisticsProviderConfiguration setHistorySize(int historySize) {
    super.setHistorySize(historySize);
    return this;
  }

  @Override
  public DefaultStatisticsProviderConfiguration setTimeToDisable(long timeToDisable, TimeUnit timeToDisableUnit) {
    super.setTimeToDisable(timeToDisable, timeToDisableUnit);
    return this;
  }

}
