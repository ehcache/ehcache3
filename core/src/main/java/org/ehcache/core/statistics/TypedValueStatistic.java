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

package org.ehcache.core.statistics;

import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.extended.StatisticType;

/**
 * Represent a {@code ValueStatistic} that knows its {@code StatisticType}.
 */
public abstract class TypedValueStatistic implements ValueStatistic<Number> {
  private final StatisticType type;

  /**
   * Type of this value statistic. Can be COUNTER or SIZE.
   *
   * @param type {@code StatisticType}
   */
  public TypedValueStatistic(StatisticType type) {
    this.type = type;
  }

  public StatisticType getType() {
    return type;
  }
}
