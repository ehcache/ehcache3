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

import java.util.Set;

import static org.ehcache.core.statistics.SuppliedValueStatistic.counter;


public interface OperationStatistic<T extends Enum<T>> extends OperationObserver<T>, SourceStatistic<ChainedOperationObserver<? super T>> {

  Class<T> type();

  /**
   * Return a {@link ValueStatistic} returning the count for the given result.
   *
   * @param result the result of interest
   * @return a {@code ValueStatistic} instance
   */
  default ValueStatistic<Long> statistic(T result) {
    return counter(() -> count(result));
  }

  default ValueStatistic<Long> statistic(Set<T> results) {
    return counter(() -> sum(results));
  }

  /**
   * Return the count of operations with the given type.
   *
   * @param type the result type
   * @return the operation count
   */
  long count(T type);

  long sum(Set<T> types);

  long sum();

}
