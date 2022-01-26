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

package org.ehcache.impl.internal.store.offheap.factories;

import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.internal.store.offheap.SwitchableEvictionAdvisor;
import org.terracotta.offheapstore.Metadata;
import org.terracotta.offheapstore.ReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.storage.StorageEngine;
import org.terracotta.offheapstore.util.Factory;

import java.nio.IntBuffer;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.locks.Lock;

/**
 * EhcacheSegmentFactory
 */
public class EhcacheSegmentFactory<K, V> implements Factory<PinnableSegment<K, V>> {

  private final Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory;
  private final PageSource tableSource;
  private final int tableSize;
  private final SwitchableEvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final EhcacheSegment.EvictionListener<K, V> evictionListener;

  public EhcacheSegmentFactory(PageSource source, Factory<? extends StorageEngine<? super K, ? super V>> storageEngineFactory, int initialTableSize, SwitchableEvictionAdvisor<? super K, ? super V> evictionAdvisor, EhcacheSegment.EvictionListener<K, V> evictionListener) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = source;
    this.tableSize = initialTableSize;
    this.evictionAdvisor = evictionAdvisor;
    this.evictionListener = evictionListener;
  }

  public PinnableSegment<K, V> newInstance() {
    StorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new EhcacheSegment<>(tableSource, storageEngine, tableSize, evictionAdvisor, evictionListener);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }

  public static class EhcacheSegment<K, V> extends ReadWriteLockedOffHeapClockCache<K, V> {

    public static final int ADVISED_AGAINST_EVICTION = 1 << (Integer.SIZE - 3);

    private final SwitchableEvictionAdvisor<? super K, ? super V> evictionAdvisor;
    private final EvictionListener<K, V> evictionListener;

    EhcacheSegment(PageSource source, StorageEngine<? super K, ? super V> storageEngine, int tableSize, SwitchableEvictionAdvisor<? super K, ? super V> evictionAdvisor, EvictionListener<K, V> evictionListener) {
      super(source, true, storageEngine, tableSize);
      this.evictionAdvisor = evictionAdvisor;
      this.evictionListener = evictionListener;
    }

    @Override
    public V put(K key, V value) {
      int metadata = getEvictionAdviceStatus(key, value);
      return put(key, value, metadata);
    }

    private int getEvictionAdviceStatus(final K key, final V value) {
      return evictionAdvisor.adviseAgainstEviction(key, value) ? ADVISED_AGAINST_EVICTION : 0;
    }

    @Override
    public V putPinned(K key, V value) {
      int metadata = getEvictionAdviceStatus(key, value) | Metadata.PINNED;
      return put(key, value, metadata);
    }

    @Override
    protected boolean evictable(int status) {
      return super.evictable(status) && (((status & ADVISED_AGAINST_EVICTION) == 0) || !evictionAdvisor.isSwitchedOn());
    }

    @Override
    public boolean evict(int index, boolean shrink) {
      Lock lock = writeLock();
      lock.lock();
      try {
        Entry<K, V> entry = getEntryAtTableOffset(index);
        boolean evicted = super.evict(index, shrink);
        if (evicted) {
          evictionListener.onEviction(entry.getKey(), entry.getValue());
        }
        return evicted;
      } finally {
        lock.unlock();
      }
    }

    @Override
    protected Set<Entry<K, V>> createEntrySet() {
      return new EntrySet();
    }

    public interface EvictionListener<K, V> {
      void onEviction(K key, V value);
    }

    private class EntrySet extends LockedEntrySet {
      @Override
      public Iterator<Entry<K, V>> iterator() {
        readLock().lock();
        try {
          return new LockedEntryIterator() {

            @Override
            protected Entry<K, V> create(IntBuffer entry) {
              Entry<K, V> entryObject = super.create(entry);
              ((Store.ValueHolder<?>) entryObject.getValue()).get();
              return entryObject;
            }
          };
        } finally {
          readLock().unlock();
        }
      }
    }
  }
}
