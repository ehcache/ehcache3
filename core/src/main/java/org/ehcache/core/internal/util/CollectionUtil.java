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
import java.util.Collections;
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

  /**
   * Create a map with one key and a corresponding value.
   *
   * @param key0 the key
   * @param value0 the value
   * @param <K> key type
   * @param <V> value type
   * @return map with one entry
   */
  public static <K, V> Map<K, V> map(K key0, V value0) {
    return Collections.singletonMap(key0, value0);
  }

  /**
   * Create a map with two key/value pairs.
   *
   * @param key0 first key
   * @param value0 first value
   * @param key1 second key
   * @param value1 second value
   * @param <K> key type
   * @param <V> value type
   * @return map with two entries
   */
  public static <K, V> Map<K, V> map(K key0, V value0, K key1, V value1) {
    Map<K, V> map = new HashMap<>(2);
    map.put(key0, value0);
    map.put(key1, value1);
    return map;
  }

  /**
   * Create a map with three key/value pairs.
   *
   * @param key0 first key
   * @param value0 first value
   * @param key1 second key
   * @param value1 second value
   * @param key2 third key
   * @param value2 third value
   * @param <K> key type
   * @param <V> value type
   * @return map with three entries
   */
  public static <K, V> Map<K, V> map(K key0, V value0, K key1, V value1, K key2, V value2) {
    Map<K, V> map = new HashMap<>(2);
    map.put(key0, value0);
    map.put(key1, value1);
    map.put(key2, value2);
    return map;
  }
}
