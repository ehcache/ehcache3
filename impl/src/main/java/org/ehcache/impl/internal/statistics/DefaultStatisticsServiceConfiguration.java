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
package org.ehcache.impl.internal.statistics;

import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.service.StatisticsServiceConfiguration;

import java.util.concurrent.TimeUnit;

public class DefaultStatisticsServiceConfiguration implements StatisticsServiceConfiguration {

  private long latencyHistoryWindow = 500;
  private TimeUnit latencyHistoryWindowUnit = TimeUnit.MILLISECONDS;
  private int latencyHistorySize = 60 * 60 * 1_000 / (int) latencyHistoryWindow; // which is equivalent to 1-hour history

  @Override
  public long getLatencyHistoryWindowInterval() {
    return latencyHistoryWindow;
  }

  @Override
  public TimeUnit getLatencyHistoryWindowUnit() {
    return latencyHistoryWindowUnit;
  }

  @Override
  public int getLatencyHistorySize() {
    return latencyHistorySize;
  }

  /**
   * The interval used to aggregate the latencies of Ehcache operations to create one latency sample.
   * Aggregation is done by keeping the maximum value that has been seen.
   * A low value (i.e. 500ms) gives more precision but more overhead.
   * <p>
   * Default value is 500ms.
   *
   * @param duration Duration of the window
   * @param unit     Duration unit
   * @return this
   */
  public DefaultStatisticsServiceConfiguration withLatencyHistoryWindow(long duration, TimeUnit unit) {
    if (duration <= 0) {
      throw new IllegalArgumentException("Window must be a positive integer");
    }
    this.latencyHistoryWindow = duration;
    this.latencyHistoryWindowUnit = unit;
    return this;
  }

  /**
   * Maximum number of latency samples kept history.
   * A sample is the maximum latency value seen over the configured window time.
   * So supposing that we have constantly get operations, the history will cover
   * a minimal time frame equivalent to the history size multiplied by the latency
   * window duration.
   * <p>
   * Default value is 7200. This means that there will be at least a 1-hour history
   * in the case where there are constant {@link org.ehcache.Cache#get(Object)} calls
   * in Ehcache with a sample window of 500ms, because 2 samples will be generated per second.
   *
   * @param size maximum history size
   * @return this
   */
  public DefaultStatisticsServiceConfiguration withLatencyHistorySize(int size) {
    if (size <= 0) {
      throw new IllegalArgumentException("Latency history size must be a positive integer");
    }
    this.latencyHistorySize = size;
    return this;
  }

  @Override
  public Class<StatisticsService> getServiceType() {
    return StatisticsService.class;
  }

}
