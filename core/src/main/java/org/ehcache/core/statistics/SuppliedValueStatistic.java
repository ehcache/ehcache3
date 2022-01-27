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

import org.terracotta.management.model.stats.StatisticType;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Supplier;


/**
 * This class can be used to create a {@link ValueStatistic} with a specific {@link StatisticType}
 * which value is given by a provided {@link Supplier}
 *
 * @author Mathieu Carbou
 */
public class SuppliedValueStatistic<T extends Serializable> implements ValueStatistic<T> {

  private final Supplier<T> supplier;
  private final StatisticType type;

  public SuppliedValueStatistic(StatisticType type, Supplier<T> supplier) {
    this.type = Objects.requireNonNull(type);
    this.supplier = Objects.requireNonNull(supplier);
  }

  @Override
  public T value() {
    return supplier.get();
  }

  @Override
  public StatisticType type() {
    return type;
  }

  public static <T extends Number> ValueStatistic<T> counter(Supplier<T> supplier) {
    return supply(StatisticType.COUNTER, supplier);
  }

  public static <T extends Serializable> ValueStatistic<T> supply(StatisticType type, Supplier<T> supplier) {
    return new SuppliedValueStatistic<>(type, supplier);
  }
}
