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
package org.ehcache.core.util;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Multiple useful methods to play with collections.
 */
public final class CollectionUtil {

  private CollectionUtil() {}

  /**
   * Used to create a new collection with the correct size. Given an iterable, will try to see it the iterable actually
   * have a size and will return it. If the iterable has no known size, we return the best bet.
   *
   * @param iterable the iterable we will try to find the size of
   * @param bestBet our best bet for the size if the iterable is not sizeable
   * @return the size of the iterable if found or null
   */
  public static int findBestCollectionSize(Iterable<?> iterable, int bestBet) {
    return (iterable instanceof Collection ? ((Collection<?>) iterable).size() : bestBet);
  }

  /**
   * Copy each map entry to a new map but check that each key and value isn't null. Throw
   * a {@code NullPointerException} if it's the case.
   *
   * @param entries entries to copy
   * @param <K> type of key
   * @param <V> type of value
   * @return cloned map
   * @throws NullPointerException if a key or value is null
   */
  public static <K, V> Map<K, V> copyMapButFailOnNull(Map<? extends K, ? extends V> entries) {
    Map<K, V> entriesToRemap = new HashMap<>(entries.size());
    entries.forEach((k, v) -> {
      // If a key/value is null, throw NPE, nothing gets mutated
      if (k == null || v == null) {
        throw new NullPointerException();
      }
      entriesToRemap.put(k, v);
    });
    return entriesToRemap;
  }
}
