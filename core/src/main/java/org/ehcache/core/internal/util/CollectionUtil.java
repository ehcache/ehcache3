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
package org.ehcache.core.internal.util;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;

/**
 * Multiple useful methods to play with collections
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
   * Return a map entry for the key and value provided.
   *
   * @param key the key of the entry
   * @param value the value of the entry
   * @param <K> type of the key
   * @param <V> type of the value
   * @return the map entry for the key and value
   */
  public static <K, V> Map.Entry<K, V> entry(K key, V value) {
    return new AbstractMap.SimpleImmutableEntry<>(key, value);
  }
}
