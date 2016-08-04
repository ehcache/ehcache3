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

import java.util.concurrent.TimeUnit;

public class DefaultStatisticsProviderConfiguration implements StatisticsProviderConfiguration {

  private final Class<? extends ManagementProvider> provider;

  private long averageWindowDuration = 60;
  private TimeUnit averageWindowUnit = TimeUnit.SECONDS;
  private int historySize = 100;
  private long historyInterval = 1;
  private TimeUnit historyIntervalUnit = TimeUnit.SECONDS;
  private long timeToDisable = 30;
  private TimeUnit timeToDisableUnit = TimeUnit.SECONDS;

  public DefaultStatisticsProviderConfiguration(Class<? extends ManagementProvider> provider, long averageWindowDuration, TimeUnit averageWindowUnit, int historySize, long historyInterval, TimeUnit historyIntervalUnit, long timeToDisable, TimeUnit timeToDisableUnit) {
    this.provider = Objects.requireNonNull(provider);
    this.averageWindowDuration = averageWindowDuration;
    this.averageWindowUnit = Objects.requireNonNull(averageWindowUnit);
    this.historySize = historySize;
    this.historyInterval = historyInterval;
    this.historyIntervalUnit = Objects.requireNonNull(historyIntervalUnit);
    this.timeToDisable = timeToDisable;
    this.timeToDisableUnit = Objects.requireNonNull(timeToDisableUnit);
  }

  public DefaultStatisticsProviderConfiguration(Class<? extends ManagementProvider> provider) {
    this.provider = Objects.requireNonNull(provider);
  }

  @Override
  public long averageWindowDuration() {
    return averageWindowDuration;
  }

  @Override
  public TimeUnit averageWindowUnit() {
    return averageWindowUnit;
  }

  @Override
  public int historySize() {
    return historySize;
  }

  @Override
  public long historyInterval() {
    return historyInterval;
  }

  @Override
  public TimeUnit historyIntervalUnit() {
    return historyIntervalUnit;
  }

  @Override
  public long timeToDisable() {
    return timeToDisable;
  }

  @Override
  public TimeUnit timeToDisableUnit() {
    return timeToDisableUnit;
  }

  @Override
  public Class<? extends ManagementProvider> getStatisticsProviderType() {
    return provider;
  }

  @Override
  public String toString() {
    return "{statisticsProviderType=" + getStatisticsProviderType() +
        ", averageWindowDuration=" + averageWindowDuration() +
        ", averageWindowUnit=" + averageWindowUnit() +
        ", historyInterval=" + historyInterval() +
        ", historyIntervalUnit=" + historyIntervalUnit() +
        ", historySize=" + historySize() +
        ", timeToDisable=" + timeToDisable() +
        ", timeToDisableUnit=" + timeToDisableUnit() +
        '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    DefaultStatisticsProviderConfiguration that = (DefaultStatisticsProviderConfiguration) o;

    if (!provider.equals(that.provider)) return false;
    if (averageWindowDuration != that.averageWindowDuration) return false;
    if (historySize != that.historySize) return false;
    if (historyInterval != that.historyInterval) return false;
    if (timeToDisable != that.timeToDisable) return false;
    if (averageWindowUnit != that.averageWindowUnit) return false;
    if (historyIntervalUnit != that.historyIntervalUnit) return false;
    return timeToDisableUnit == that.timeToDisableUnit;

  }

  @Override
  public int hashCode() {
    int result = (int) (averageWindowDuration ^ (averageWindowDuration >>> 32));
    result = 31 * result + provider.hashCode();
    result = 31 * result + averageWindowUnit.hashCode();
    result = 31 * result + historySize;
    result = 31 * result + (int) (historyInterval ^ (historyInterval >>> 32));
    result = 31 * result + historyIntervalUnit.hashCode();
    result = 31 * result + (int) (timeToDisable ^ (timeToDisable >>> 32));
    result = 31 * result + timeToDisableUnit.hashCode();
    return result;
  }

  public DefaultStatisticsProviderConfiguration setAverageWindowDuration(long averageWindowDuration) {
    this.averageWindowDuration = averageWindowDuration;
    return this;
  }

  public DefaultStatisticsProviderConfiguration setAverageWindowUnit(TimeUnit averageWindowUnit) {
    this.averageWindowUnit = averageWindowUnit;
    return this;
  }

  public DefaultStatisticsProviderConfiguration setHistoryInterval(long historyInterval) {
    this.historyInterval = historyInterval;
    return this;
  }

  public DefaultStatisticsProviderConfiguration setHistoryIntervalUnit(TimeUnit historyIntervalUnit) {
    this.historyIntervalUnit = historyIntervalUnit;
    return this;
  }

  public DefaultStatisticsProviderConfiguration setHistorySize(int historySize) {
    this.historySize = historySize;
    return this;
  }

  public DefaultStatisticsProviderConfiguration setTimeToDisable(long timeToDisable) {
    this.timeToDisable = timeToDisable;
    return this;
  }

  public DefaultStatisticsProviderConfiguration setTimeToDisableUnit(TimeUnit timeToDisableUnit) {
    this.timeToDisableUnit = timeToDisableUnit;
    return this;
  }

}
