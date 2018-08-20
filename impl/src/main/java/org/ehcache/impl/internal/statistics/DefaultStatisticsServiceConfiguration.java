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

import java.time.Duration;

public class DefaultStatisticsServiceConfiguration implements StatisticsServiceConfiguration {

  private Boolean calculateLatencies;
  private Duration defaultHistogramWindow = Duration.ofMinutes(1);

  @Override
  public Duration getDefaultHistogramWindow() {
    return defaultHistogramWindow;
  }

  @Override
  public Boolean getCalculateLatencies() {
    return calculateLatencies;
  }

  /**
   * @param duration the default histogram window size
   * @return this
   */
  public DefaultStatisticsServiceConfiguration withDefaultHistogramWindow(Duration duration) {
    this.defaultHistogramWindow = duration;
    return this;
  }

  /**
   * @param calculate if latencies should be calculated. Null means that default behavior will be used.
   * @return this
   */
  public DefaultStatisticsServiceConfiguration withCalculateLatencies(Boolean calculate) {
    this.calculateLatencies = calculate;
    return this;
  }

  @Override
  public Class<StatisticsService> getServiceType() {
    return StatisticsService.class;
  }

}
