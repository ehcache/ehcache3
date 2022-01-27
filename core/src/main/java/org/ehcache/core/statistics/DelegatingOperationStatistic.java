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

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class DelegatingOperationStatistic<T extends Enum<T>> implements OperationStatistic<T> {

  private final org.terracotta.statistics.OperationStatistic<T> delegate;

  public DelegatingOperationStatistic(org.terracotta.statistics.OperationStatistic<T> statistic) {
    this.delegate = statistic;
  }

  @Override
  public Class<T> type() {
    return delegate.type();
  }

  @Override
  public long count(T type) {
    return delegate.count(type);
  }

  @Override
  public long sum(Set<T> types) {
    return delegate.sum(types);
  }

  @Override
  public long sum() {
    return delegate.sum();
  }

  @Override
  public void begin() {
    delegate.begin();
  }

  @Override
  public void end(T result) {
    delegate.end(result);
  }

  @Override
  public void addDerivedStatistic(ChainedOperationObserver<? super T> derived) {
    delegate.addDerivedStatistic(convert(derived));
  }

  @Override
  public void removeDerivedStatistic(ChainedOperationObserver<? super T> derived) {
    delegate.removeDerivedStatistic(convert(derived));
  }

  @Override
  public Collection<ChainedOperationObserver<? super T>> getDerivedStatistics() {
    Collection<org.terracotta.statistics.observer.ChainedOperationObserver<? super T>> derivedStatistics = delegate.getDerivedStatistics();
    return derivedStatistics.stream().map(this::revert).collect(Collectors.toSet());
  }

  private ChainedOperationObserver<? super T> revert(org.terracotta.statistics.observer.ChainedOperationObserver<? super T> observer) {
    return new ChainedOperationObserver<T>() {
      @Override
      public void begin(long time) {
        observer.begin(time);
      }

      @Override
      public void end(long time, long latency, T result) {
        observer.end(time, latency, result);
      }
    };
  }

  private org.terracotta.statistics.observer.ChainedOperationObserver<T> convert(ChainedOperationObserver<? super T> observer) {
    return new org.terracotta.statistics.observer.ChainedOperationObserver<T>() {
      @Override
      public void begin(long time) {
        observer.begin(time);
      }

      @Override
      public void end(long time, long latency, T result) {
        observer.end(time, latency, result);
      }
    };
  }
}
