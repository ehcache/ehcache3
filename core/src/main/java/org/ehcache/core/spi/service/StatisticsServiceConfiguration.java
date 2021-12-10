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
package org.ehcache.core.spi.service;

import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.util.concurrent.TimeUnit;

public interface StatisticsServiceConfiguration extends ServiceCreationConfiguration<StatisticsService> {

  /**
   * Aggregation is done by keeping the maximum value that has been seen.
   * A low value (i.e. 500ms) is better.
   *
   * @return The interval used to aggregate the latencies of Ehcache operations to create one latency sample.
   */
  long getLatencyHistoryWindowInterval();

  /**
   * @return the unit for the latency history interval
   */
  TimeUnit getLatencyHistoryWindowUnit();

  /**
   * @return the maximum number of maximum latency samples to keep.
   */
  int getLatencyHistorySize();

}
