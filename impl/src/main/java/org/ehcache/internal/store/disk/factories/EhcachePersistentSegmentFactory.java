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

package org.ehcache.internal.store.disk.factories;

import org.ehcache.function.BiFunction;
import org.ehcache.function.Function;
import org.ehcache.function.Predicate;
import org.terracotta.offheapstore.Metadata;
import org.terracotta.offheapstore.Segment;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.persistent.PersistentReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.disk.persistent.PersistentStorageEngine;
import org.terracotta.offheapstore.util.Factory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;

/**
 *
 * @author Chris Dennis
 */
public class EhcachePersistentSegmentFactory<K, V> implements Factory<Segment<K, V>> {

  private final Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory;
  private final MappedPageSource tableSource;
  private final int tableSize;

  private final Predicate<Map.Entry<K, V>> evictionVeto;
  private final EhcachePersistentSegment.EvictionListener<K, V> evictionListener;

  
  public EhcachePersistentSegmentFactory(MappedPageSource source, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, int initialTableSize, Predicate<Map.Entry<K, V>> evictionVeto, EhcachePersistentSegment.EvictionListener<K, V> evictionListener) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = source;
    this.tableSize = initialTableSize;
    this.evictionVeto = evictionVeto;
    this.evictionListener = evictionListener;
  }

  public EhcachePersistentSegment<K, V> newInstance() {
    PersistentStorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new EhcachePersistentSegment<K, V>(tableSource, storageEngine, tableSize, true, evictionVeto, evictionListener);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }

  public static class EhcachePersistentSegment<K, V> extends PersistentReadWriteLockedOffHeapClockCache<K, V> {

    private final Predicate<Entry<K, V>> evictionVeto;
    private final EvictionListener<K, V> evictionListener;

    EhcachePersistentSegment(MappedPageSource source, PersistentStorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap, Predicate<Entry<K, V>> evictionVeto, EvictionListener<K, V> evictionListener) {
      super(source, storageEngine, tableSize, bootstrap);
      this.evictionVeto = evictionVeto;
      this.evictionListener = evictionListener;
    }

    @Override
    public boolean evict(int index, boolean shrink) {
      Lock l = writeLock();
      l.lock();
      try {
        Entry<K, V> entry = getEntryAtTableOffset(index);
        boolean evicted = super.evict(index, shrink);
        if (evicted) {
          evictionListener.onEviction(entry.getKey(), entry.getValue());
        }
        return evicted;
      } finally {
        l.unlock();
      }
    }

    @Override
    public V put(K key, V value, int metadata) {
      Lock lock = writeLock();
      lock.lock();
      try {
        return super.put(key, value, metadata);
      } finally {
        lock.unlock();
      }
    }

    @Override
    public V remove(Object key) {
      Lock lock = writeLock();
      lock.lock();
      try {
        return super.remove(key);
      } finally {
        lock.unlock();
      }
    }

    /**
     * Computes a new value for the given key if a mapping is present and pinned, <code>BiFunction</code> is invoked under appropriate lock scope
     * @param key the key of the mapping to compute the value for
     * @param remappingFunction the function used to compute
     * @return true if transitioned to unpinned, false otherwise
     */
    public boolean computeIfPinnedAndUnpin(final K key, final BiFunction<K, V, V> remappingFunction) {
      final Lock lock = writeLock();
      lock.lock();
      try {
        final V newValue;
        // can't be pinned if absent
        if ((getMetadata(key) & Metadata.PINNED) == Metadata.PINNED) {

          final V previousValue = get(key);
          newValue = remappingFunction.apply(key, previousValue);

          if(newValue != previousValue) {
            if(newValue == null) {
              remove(key);
            } else {
              put(key, newValue);
            }
          }
          if (newValue != null) {
            setMetadata(key, Metadata.PINNED, 0);
          }
          return true;
        }
        return false;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Computes a new mapping for the given key by calling the function passed in. It will pin the mapping
     * if the flag is true, it will however not unpin an existing pinned mapping in case the function returns
     * the existing value.
     *
     * @param key the key to compute the mapping for
     * @param mappingFunction the function to compute the mapping
     * @param pin pins the mapping if {code true}
     *
     * @return the mapped value
     */
    public V compute(K key, BiFunction<K, V, V> mappingFunction, boolean pin) {
      Lock lock = writeLock();
      lock.lock();
      try {
        V value = get(key);
        V newValue = mappingFunction.apply(key, value);
        if (newValue != null) {
          if (newValue != value) {
            put(key, newValue);
          }
          if (pin) {
            setPinning(key, true);
          }
        } else {
          remove(key);
        }
        return newValue;
      } finally {
        lock.unlock();
      }
    }

    /**
     * Computes a new mapping for the given key only if a mapping existed already by calling the function passed in.
     *
     * @param key the key to compute the mapping for
     * @param mappingFunction the function to compute the mapping
     *
     * @return the mapped value
     */
    public V computeIfPresent(K key, BiFunction<K, V, V> mappingFunction) {
      Lock lock = writeLock();
      lock.lock();
      try {
        V value = get(key);
        if (value != null) {
          V newValue = mappingFunction.apply(key, value);
          if (newValue != value) {
            if (newValue != null) {
              put(key, newValue);
            } else {
              remove(key);
            }
          }
          return newValue;
        } else {
          return null;
        }
      } finally {
        lock.unlock();
      }
    }

    /**
     * Computes a new value for the given key if a mapping is present and pinned, <code>BiFunction</code> is invoked under appropriate lock scope
     * The pinning bit from the metadata, will be flipped (i.e. unset) if the <code>Function<V, Boolean></code> returns true
     * @param key the key of the mapping to compute the value for
     * @param remappingFunction the function used to compute
     * @param flippingPinningBitFunction evaluated to see whether we want to unpin the mapping
     * @return true if transitioned to unpinned, false otherwise
     */
    public boolean computeIfPinned(final K key, final BiFunction<K, V, V> remappingFunction, final Function<V, Boolean> flippingPinningBitFunction) {
      final Lock lock = writeLock();
      lock.lock();
      try {
        final V newValue;
        // can't be pinned if absent
        if ((getMetadata(key) & Metadata.PINNED) == Metadata.PINNED) {

          final V previousValue = get(key);
          newValue = remappingFunction.apply(key, previousValue);

          if(newValue != previousValue) {
            if(newValue == null) {
              remove(key);
            } else {
              put(key, newValue);
            }
          }
          if (flippingPinningBitFunction.apply(previousValue)) {
            setMetadata(key, Metadata.PINNED, 0);
            return true;
          }
        }
        return false;
      } finally {
        lock.unlock();
      }
    }

    public interface EvictionListener<K, V> {
      void onEviction(K key, V value);
    }

  }
}
