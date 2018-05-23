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
package org.ehcache.impl.internal.concurrent;

import org.ehcache.config.EvictionAdvisor;

import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentMap;

public interface EvictingConcurrentMap<K, V> extends ConcurrentMap<K, V>{

  /**
   * Return the preferred entry to evict based on a sample of entries taken from the map.
   *
   * @param rndm Random implementation used to determine the sample randomly
   * @param size Number of sampled entries
   * @param prioritizer Prioritizer used to determine the best entry to evict in the sample
   * @param evictionAdvisor Can veto against the eviction of an entry
   * @return Entry to evict or null is none was found
   */
  Entry<K, V> getEvictionCandidate(Random rndm, int size, Comparator<? super V> prioritizer, EvictionAdvisor<? super K, ? super V> evictionAdvisor);

  /**
   * Returns the number of mappings. This method should be used
   * instead of {@link #size} because a ConcurrentHashMap may
   * contain more mappings than can be represented as an int. The
   * value returned is an estimate; the actual count may differ if
   * there are concurrent insertions or removals.
   *
   * @return the number of mappings
   */
  long mappingCount();

  /**
   * Remove all entries for a given hashcode (as returned by {@code key.hashCode()}).
   *
   * @param keyHash remove entries having this hashcode
   * @return the removed entries
   */
  Collection<Entry<K, V>> removeAllWithHash(int keyHash);
}
