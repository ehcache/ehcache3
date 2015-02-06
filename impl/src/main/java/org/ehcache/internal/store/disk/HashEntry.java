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

package org.ehcache.internal.store.disk;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Internal entry structure used by the {@link Segment} class.
 *
 * @author Chris Dennis
 * @author Ludovic Orban
 */
final class HashEntry<K, V> {

  /**
   * Key instance for this mapping.
   */
  protected final K key;

  /**
   * Spread hash value for they key.
   */
  protected final int hash;

  /**
   * Reference to the next HashEntry in this chain.
   */
  protected final HashEntry<K, V> next;

  /**
   * Reference to the DiskSubstitute for this entry.
   */
  protected volatile DiskStorageFactory.DiskSubstitute<K, V> element;

  /**
   * marks the entry as faulted, can't evict faulted things
   */
  protected final AtomicBoolean faulted;

  /**
   * Constructs a HashEntry instance mapping the supplied key, value pair
   * and linking it to the supplied HashEntry
   *
   * @param key     key for this entry
   * @param hash    spread-hash for this entry
   * @param next    next HashEntry in the chain
   * @param element initial value for this entry
   */
  HashEntry(K key, int hash, HashEntry<K, V> next, DiskStorageFactory.DiskSubstitute<K, V> element, AtomicBoolean faulted) {
    this.key = key;
    this.hash = hash;
    this.next = next;
    this.element = element;
    this.faulted = faulted;
  }
}
