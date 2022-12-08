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

import java.util.Set;

import org.terracotta.statistics.OperationStatistic;
import org.terracotta.statistics.ValueStatistic;
import org.terracotta.statistics.observer.ChainedOperationObserver;

/**
 * An operation statistic that always return 0 for everything. Used as a null object.
 */
class ZeroOperationStatistic<T extends Enum<T>> implements OperationStatistic<T> {

  @SuppressWarnings("rawtypes")
  private static final OperationStatistic<?> INSTANCE = new ZeroOperationStatistic();

  @SuppressWarnings("unchecked")
  public static <T extends Enum<T>> OperationStatistic<T> get() {
    return (OperationStatistic<T>) INSTANCE;
  }

  @Override
  public Class<T> type() {
    return null;
  }

  @Override
  public ValueStatistic<Long> statistic(T result) {
    return null;
  }

  @Override
  public ValueStatistic<Long> statistic(Set<T> results) {
    return null;
  }

  @Override
  public long count(T type) {
    return 0;
  }

  @Override
  public long sum(Set<T> types) {
    return 0;
  }

  @Override
  public long sum() {
    return 0;
  }

  @Override
  public void addDerivedStatistic(ChainedOperationObserver<? super T> derived) {

  }

  @Override
  public void removeDerivedStatistic(ChainedOperationObserver<? super T> derived) {

  }

  @Override
  public void begin() {

  }

  @Override
  public void end(T result) {

  }

  @Override
  public void end(T result, long... parameters) {

  }
}
