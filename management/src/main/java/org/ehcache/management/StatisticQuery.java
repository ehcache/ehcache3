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
package org.ehcache.management;

import org.terracotta.management.stats.ContextualStatistics;

import java.util.Collection;

/**
 * @author Mathieu Carbou
 */
public interface StatisticQuery extends Query<ContextualStatistics> {

  /**
   * @return The list of statistic queried
   */
  Collection<String> getStatisticNames();

  /**
   * The query will only collect statistics computed since this time.
   * This allows for example to filter out the list of returned values within some {@link org.terracotta.statistics.Statistic}
   * such as {@link org.terracotta.management.stats.history.RateHistory}
   *
   * @return A unix timestamp
   */
  long getSince();

  interface Builder extends QueryBuilder<Builder, StatisticQuery> {

    /**
     * The query will only collect statistics computed since this time.
     * This allows for example to filter out the list of returned values within some {@link org.terracotta.statistics.Statistic}
     * such as {@link org.terracotta.management.stats.history.RateHistory}
     *
     * @param unixTimestampMs The unix timestamp
     * @return this builder
     */
    Builder since(long unixTimestampMs);

  }

}
