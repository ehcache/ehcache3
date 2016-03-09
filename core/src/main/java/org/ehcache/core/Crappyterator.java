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
package org.ehcache.core;

import org.ehcache.Cache;
import org.ehcache.core.spi.cache.Store;
import org.ehcache.exceptions.CacheAccessException;

import java.util.Iterator;

/**
 * The reason behind this class' name is because the 107 standard is so fucked up in so many mixed aspects
 * <b>and</b> its TCK is so ridiculously relying on implementation details when it actually does test something
 * remotely useful, Ehcache's 'sane' iterator contract (whatever sane might mean in this context) makes
 * the 107 TCK unhappy.
 *
 * The 107 iteration contract combined with stats and expiration is so mind-blowingly lame that there is no way
 * to adapt Ehcache's iterator. A secondary, really, really crappy one must be implemented.
 *
 * @author Ludovic Orban
 */
class Crappyterator<K, V> implements Iterator<Cache.Entry<K, V>> {

  private final InternalCache<K, V> cache;
  private final Store<K, V> store;
  private final Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator;
  private Cache.Entry<K, Store.ValueHolder<V>> current;

  public Crappyterator(InternalCache<K, V> cache, Store<K, V> store) {
    this.cache = cache;
    this.store = store;
    this.iterator = store.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  /**
   * The multiple get calls are a necessary stupidity to please the 107 TCK's stats and
   * you-cannot-expire-on-creation-but-immediately-expire-upon-retrieval-but-not-the-first-one-of-course contract.
   */
  @Override
  public Cache.Entry<K, V> next() {
    try {
      final Cache.Entry<K, Store.ValueHolder<V>> next = iterator.next();
      // call Cache.get() here to check for expiry *and* account for a get in the stats, without using the loader
      if (cache.getNoLoader(next.getKey()) == null) {
        current = null;
        return null;
      }
      // call Store.get() here to check for hasNext()' expiry *and do not* account for an extra get in the stats
      // TODO: this get can be optimized away if the stores expire mappings upon metadata update
      // that would also get rid of that ugly try-catch that doesn't call the resilience strategy with the CacheAccessException
      store.get(next.getKey());
      current = next;

      final K key = next.getKey();
      final V value = next.getValue().value();
      return new Cache.Entry<K, V>() {
        @Override
        public K getKey() {
          return key;
        }

        @Override
        public V getValue() {
          return value;
        }
      };
    } catch (CacheAccessException cae) {
      current = null;
      return null;
    }
  }

  @Override
  public void remove() {
    if (current == null) {
      throw new IllegalStateException();
    }
    // TODO: should be 2-args remove for correctness, but that breaks the TCK's stats tests
    // as Iterator.remove() must be accounted as a remove but not as a get, while 2-args remove successes must be
    // accounted as both a get and a remove
    cache.remove(current.getKey());
    current = null;
  }
}
