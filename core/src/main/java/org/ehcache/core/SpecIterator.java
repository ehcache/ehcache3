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
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.StoreAccessException;

import java.util.Iterator;

/**
 * This implementation could change depending on <a href="https://github.com/jsr107/jsr107spec/issues/337">jsr107spec issue #337</a>
 * as letting next() return true might not be such a great idea, and too strict accounting of stats in the TCK
 * preventing a look-ahead iterator implementation.
 *
 * @author Ludovic Orban
 */
class SpecIterator<K, V> implements Iterator<Cache.Entry<K, V>> {

  private final Jsr107Cache<K, V> cache;
  private final Store.Iterator<Cache.Entry<K, Store.ValueHolder<V>>> iterator;
  private Cache.Entry<K, Store.ValueHolder<V>> current;

  public SpecIterator(Jsr107Cache<K, V> cache, Store<K, V> store) {
    this.cache = cache;
    this.iterator = store.iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Cache.Entry<K, V> next() {
    try {
      Cache.Entry<K, Store.ValueHolder<V>> next = iterator.next();
      final K nextKey = next.getKey();
      Store.ValueHolder<V> nextValueHolder = next.getValue();

      // call Cache.get() here to check for expiry *and* account for a get in the stats, without using the loader
      if (cache.getNoLoader(nextKey) == null) {
        current = null;
        return null;
      }

      current = next;

      final V nextValue = nextValueHolder.value();
      return new Cache.Entry<K, V>() {
        @Override
        public K getKey() {
          return nextKey;
        }

        @Override
        public V getValue() {
          return nextValue;
        }
      };
    } catch (StoreAccessException sae) {
      current = null;
      return null;
    }
  }

  @Override
  public void remove() {
    if (current == null) {
      throw new IllegalStateException();
    }
    cache.remove(current.getKey());
    current = null;
  }
}
