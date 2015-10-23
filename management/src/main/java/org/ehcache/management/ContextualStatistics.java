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

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * This class holds the {@link Statistic} list quered from a specific context
 *
 * @author Mathieu Carbou
 */
public final class ContextualStatistics implements Iterable<Statistic<?, ?>> {

  private final Map<String, Statistic<?, ?>> statistics;
  private final Context context;

  public ContextualStatistics(Context context, Map<String, Statistic<?, ?>> statistics) {
    this.statistics = statistics;
    this.context = context;
  }

  public int size() {return statistics.size();}

  public boolean isEmpty() {return statistics.isEmpty();}

  @Override
  public Iterator<Statistic<?, ?>> iterator() {
    return statistics.values().iterator();
  }

  public Map<String, Statistic<?, ?>> getStatistics() {
    return statistics;
  }

  /**
   * Returns the only possible statistic for a specific type
   *
   * @param type The type of the statistic to return
   * @param <T>  The {@link Statistic} type
   * @return The statistic found
   * @throws NoSuchElementException If there is 0 or more than 1 statistic for given type
   */
  public <T extends Statistic<?, ?>> T getStatistic(Class<T> type) throws NoSuchElementException {
    Map<String, T> filtered = getStatistics(type);
    if (filtered.size() != 1) {
      throw new NoSuchElementException(type.getName());
    }
    return filtered.values().iterator().next();
  }

  /**
   * Returns the only possible statistic for a specific type and name
   *
   * @param type The type of the statistic to return
   * @param name The name of the statistic to return
   * @param <T>  The {@link Statistic} type
   * @return The statistic found
   * @throws NoSuchElementException If there is 0 or more than 1 statistic for given type
   */
  public <T extends Statistic<?, ?>> T getStatistic(Class<T> type, String name) throws NoSuchElementException {
    Map<String, T> filtered = getStatistics(type);
    for (T statistic : filtered.values()) {
      if(statistic.getName().equals(name)) {
        return statistic;
      }
    }
    throw new NoSuchElementException(name + ":" + type.getName());
  }

  public <T extends Statistic<?, ?>> Map<String, T> getStatistics(Class<T> type) {
    Map<String, T> filtered = new LinkedHashMap<String, T>();
    for (Map.Entry<String, Statistic<?, ?>> entry : statistics.entrySet()) {
      if (type.isInstance(entry.getValue())) {
        filtered.put(entry.getKey(), type.cast(entry.getValue()));
      }
    }
    return filtered;
  }

  public Context getContext() {
    return context;
  }

}
