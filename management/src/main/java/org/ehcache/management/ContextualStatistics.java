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

import org.terracotta.management.stats.Statistic;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This class holds the {@link Statistic} list queried from a specific context
 *
 * @author Mathieu Carbou
 */
public final class ContextualStatistics implements Iterable<Statistic<?>> {

  private final List<Statistic<?>> statistics;
  private final Map<String, String> context;

  public ContextualStatistics(Map<String, String> context, List<Statistic<?>> statistics) {
    this.statistics = Collections.unmodifiableList(statistics);
    this.context = Collections.unmodifiableMap(context);
  }

  @Override
  public Iterator<Statistic<?>> iterator() {
    return statistics.iterator();
  }

  public List<Statistic<?>> getStatistics() {
    return statistics;
  }

  public <T extends Statistic<?>> T getStatistic(int index, Class<T> type) {
    return type.cast(statistics.get(index));
  }

  public <T extends Statistic<?>> List<T> getStatistics(Class<T> type) {
    List<T> filtered = new ArrayList<T>();
    for (Statistic<?> statistic : statistics) {
      if (type.isInstance(statistic)) {
        filtered.add(type.cast(statistic));
      }
    }
    return filtered;
  }

  public Map<String, String> getContext() {
    return context;
  }

}
