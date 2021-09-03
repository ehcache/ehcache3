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
import java.util.Collections;
import java.util.Set;

public class ZeroOperationStatistic<T extends Enum<T>> implements OperationStatistic<T> {

  private static final OperationStatistic<?> INSTANCE = new ZeroOperationStatistic<>();

  @SuppressWarnings("unchecked")
  public static <T extends Enum<T>> OperationStatistic<T> get() {
    return (OperationStatistic<T>) INSTANCE;
  }

  @Override
  public Class<T> type() {
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
  public Collection<ChainedOperationObserver<? super T>> getDerivedStatistics() {
    return Collections.emptyList();
  }

  @Override
  public void begin() {

  }

  @Override
  public void end(T result) {

  }
}
