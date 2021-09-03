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
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author Mathieu Carbou
 */
public class DefaultStatisticsServiceConfigurationTest {

  DefaultStatisticsServiceConfiguration configuration = new DefaultStatisticsServiceConfiguration();

  @Test
  public void getLatencyHistoryWindowInterval() {
    assertThat(configuration.getLatencyHistoryWindowInterval()).isEqualTo(500L);
  }

  @Test
  public void getLatencyHistoryWindowUnit() {
    assertThat(configuration.getLatencyHistoryWindowUnit()).isEqualTo(MILLISECONDS);
  }

  @Test
  public void getLatencyHistorySize() {
    assertThat(configuration.getLatencyHistorySize()).isEqualTo(7200);
  }

  @Test
  public void withLatencyHistoryWindow() {
    configuration.withLatencyHistoryWindow(1, SECONDS);
    assertThat(configuration.getLatencyHistoryWindowInterval()).isEqualTo(1L);
    assertThat(configuration.getLatencyHistoryWindowUnit()).isEqualTo(SECONDS);
  }

  @Test
  public void withLatencyHistorySize() {
    configuration.withLatencyHistorySize(60);
    assertThat(configuration.getLatencyHistorySize()).isEqualTo(60);
  }

  @Test
  public void getServiceType() {
    assertThat(configuration.getServiceType()).isEqualTo(StatisticsService.class);
  }
}
