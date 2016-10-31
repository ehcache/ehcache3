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
package org.ehcache.clustered.server.management;

import org.terracotta.management.model.Objects;
import org.terracotta.management.registry.ManagementProvider;

import java.util.concurrent.TimeUnit;

class StatisticConfiguration {

  private long averageWindowDuration = 60;
  private TimeUnit averageWindowUnit = TimeUnit.SECONDS;
  private int historySize = 100;
  private long historyInterval = 1;
  private TimeUnit historyIntervalUnit = TimeUnit.SECONDS;
  private long timeToDisable = 30;
  private TimeUnit timeToDisableUnit = TimeUnit.SECONDS;

  StatisticConfiguration() {
  }

  StatisticConfiguration(long averageWindowDuration, TimeUnit averageWindowUnit, int historySize, long historyInterval, TimeUnit historyIntervalUnit, long timeToDisable, TimeUnit timeToDisableUnit) {
    this.averageWindowDuration = averageWindowDuration;
    this.averageWindowUnit = Objects.requireNonNull(averageWindowUnit);
    this.historySize = historySize;
    this.historyInterval = historyInterval;
    this.historyIntervalUnit = Objects.requireNonNull(historyIntervalUnit);
    this.timeToDisable = timeToDisable;
    this.timeToDisableUnit = Objects.requireNonNull(timeToDisableUnit);
  }

  public long averageWindowDuration() {
    return averageWindowDuration;
  }

  public TimeUnit averageWindowUnit() {
    return averageWindowUnit;
  }

  public int historySize() {
    return historySize;
  }

  public long historyInterval() {
    return historyInterval;
  }

  public TimeUnit historyIntervalUnit() {
    return historyIntervalUnit;
  }

  public long timeToDisable() {
    return timeToDisable;
  }

  public TimeUnit timeToDisableUnit() {
    return timeToDisableUnit;
  }


  @Override
  public String toString() {
    return "{averageWindowDuration=" + averageWindowDuration() +
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
    StatisticConfiguration that = (StatisticConfiguration) o;
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
    result = 31 * result + averageWindowUnit.hashCode();
    result = 31 * result + historySize;
    result = 31 * result + (int) (historyInterval ^ (historyInterval >>> 32));
    result = 31 * result + historyIntervalUnit.hashCode();
    result = 31 * result + (int) (timeToDisable ^ (timeToDisable >>> 32));
    result = 31 * result + timeToDisableUnit.hashCode();
    return result;
  }

  public StatisticConfiguration setAverageWindowDuration(long averageWindowDuration) {
    this.averageWindowDuration = averageWindowDuration;
    return this;
  }

  public StatisticConfiguration setAverageWindowUnit(TimeUnit averageWindowUnit) {
    this.averageWindowUnit = averageWindowUnit;
    return this;
  }

  public StatisticConfiguration setHistoryInterval(long historyInterval) {
    this.historyInterval = historyInterval;
    return this;
  }

  public StatisticConfiguration setHistoryIntervalUnit(TimeUnit historyIntervalUnit) {
    this.historyIntervalUnit = historyIntervalUnit;
    return this;
  }

  public StatisticConfiguration setHistorySize(int historySize) {
    this.historySize = historySize;
    return this;
  }

  public StatisticConfiguration setTimeToDisable(long timeToDisable) {
    this.timeToDisable = timeToDisable;
    return this;
  }

  public StatisticConfiguration setTimeToDisableUnit(TimeUnit timeToDisableUnit) {
    this.timeToDisableUnit = timeToDisableUnit;
    return this;
  }

}
