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

import org.terracotta.statistics.MappedOperationStatistic;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class DelegatedMappedOperationStatistics<S extends Enum<S>, D extends Enum<D>> implements OperationStatistic<D> {

  private final MappedOperationStatistic<S, D> delegate;

  public DelegatedMappedOperationStatistics(MappedOperationStatistic<S, D> operationStatistic) {
    this.delegate = operationStatistic;
  }

  @Override
  public Class<D> type() {
    return delegate.type();
  }

  @Override
  public long count(D type) {
    return delegate.count(type);
  }

  @Override
  public long sum(Set<D> types) {
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
  public void end(D result) {
    delegate.end(result);
  }

  @Override
  public void addDerivedStatistic(ChainedOperationObserver<? super D> derived) {
    delegate.addDerivedStatistic(convert(derived));
  }

  @Override
  public void removeDerivedStatistic(ChainedOperationObserver<? super D> derived) {
    delegate.removeDerivedStatistic(convert(derived));
  }

  @Override
  public Collection<ChainedOperationObserver<? super D>> getDerivedStatistics() {
    Collection<org.terracotta.statistics.observer.ChainedOperationObserver<? super D>> derivedStatistics = delegate.getDerivedStatistics();
    return derivedStatistics.stream().map(this::revert).collect(Collectors.toSet());
  }

  private ChainedOperationObserver<? super D> revert(org.terracotta.statistics.observer.ChainedOperationObserver<? super D> observer) {
    return new ChainedOperationObserver<D>() {
      @Override
      public void begin(long time) {
        observer.begin(time);
      }

      @Override
      public void end(long time, long latency, D result) {
        observer.end(time, latency, result);
      }
    };
  }

  private org.terracotta.statistics.observer.ChainedOperationObserver<D> convert(ChainedOperationObserver<? super D> observer) {
    return new org.terracotta.statistics.observer.ChainedOperationObserver<D>() {
      @Override
      public void begin(long time) {
        observer.begin(time);
      }

      @Override
      public void end(long time, long latency, D result) {
        observer.end(time, latency, result);
      }
    };
  }


}
