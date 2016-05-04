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

package org.ehcache.impl.internal.store.disk.factories;

import org.ehcache.config.EvictionAdvisor;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment;
import org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.EvictionListener;
import org.terracotta.offheapstore.Metadata;
import org.terracotta.offheapstore.disk.paging.MappedPageSource;
import org.terracotta.offheapstore.disk.persistent.PersistentReadWriteLockedOffHeapClockCache;
import org.terracotta.offheapstore.disk.persistent.PersistentStorageEngine;
import org.terracotta.offheapstore.pinning.PinnableSegment;
import org.terracotta.offheapstore.util.Factory;

import java.util.concurrent.locks.Lock;

import static org.ehcache.impl.internal.store.offheap.factories.EhcacheSegmentFactory.EhcacheSegment.ADVISED_AGAINST_EVICTION;

/**
 *
 * @author Chris Dennis
 */
public class EhcachePersistentSegmentFactory<K, V> implements Factory<PinnableSegment<K, V>> {

  private final Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory;
  private final MappedPageSource tableSource;
  private final int tableSize;

  private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
  private final EhcacheSegment.EvictionListener<K, V> evictionListener;

  private final boolean bootstrap;

  public EhcachePersistentSegmentFactory(MappedPageSource source, Factory<? extends PersistentStorageEngine<? super K, ? super V>> storageEngineFactory, int initialTableSize, EvictionAdvisor<? super K, ? super V> evictionAdvisor, EhcacheSegment.EvictionListener<K, V> evictionListener, boolean bootstrap) {
    this.storageEngineFactory = storageEngineFactory;
    this.tableSource = source;
    this.tableSize = initialTableSize;
    this.evictionAdvisor = evictionAdvisor;
    this.evictionListener = evictionListener;
    this.bootstrap = bootstrap;
  }

  public EhcachePersistentSegment<K, V> newInstance() {
    PersistentStorageEngine<? super K, ? super V> storageEngine = storageEngineFactory.newInstance();
    try {
      return new EhcachePersistentSegment<K, V>(tableSource, storageEngine, tableSize, bootstrap, evictionAdvisor, evictionListener);
    } catch (RuntimeException e) {
      storageEngine.destroy();
      throw e;
    }
  }

  public static class EhcachePersistentSegment<K, V> extends PersistentReadWriteLockedOffHeapClockCache<K, V> {

    private final EvictionAdvisor<? super K, ? super V> evictionAdvisor;
    private final EvictionListener<K, V> evictionListener;

    EhcachePersistentSegment(MappedPageSource source, PersistentStorageEngine<? super K, ? super V> storageEngine, int tableSize, boolean bootstrap, EvictionAdvisor<? super K, ? super V> evictionAdvisor, EvictionListener<K, V> evictionListener) {
      super(source, storageEngine, tableSize, bootstrap);
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
      return super.evictable(status) && ((status & ADVISED_AGAINST_EVICTION) == 0);
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
  }
}
