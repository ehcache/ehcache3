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
package org.ehcache.clustered.server.offheap;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.ServerStore;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerStoreEventListener;
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.terracotta.offheapstore.MapInternals;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

public class OffHeapServerStore implements ServerStore, MapInternals {

  private static final long MAX_PAGE_SIZE_IN_KB = KILOBYTES.convert(8, MEGABYTES);

  private final List<OffHeapChainMap<Long>> segments;
  private final KeySegmentMapper mapper;
  private volatile ServerStoreEventListener listener;
  private volatile boolean fireEvents;

  public OffHeapServerStore(List<OffHeapChainMap<Long>> segments, KeySegmentMapper mapper) {
    this.mapper = mapper;
    this.segments = segments;
  }

  OffHeapServerStore(PageSource source, KeySegmentMapper mapper, boolean writeBehindConfigured) {
    this.mapper = mapper;
    segments = new ArrayList<>(mapper.getSegments());
    for (int i = 0; i < mapper.getSegments(); i++) {
      if (writeBehindConfigured) {
        segments.add(new PinningOffHeapChainMap<>(source, LongPortability.INSTANCE, KILOBYTES.toBytes(4), MEGABYTES.toBytes(8), false));
      } else {
        segments.add(new OffHeapChainMap<>(source, LongPortability.INSTANCE, KILOBYTES.toBytes(4), MEGABYTES.toBytes(8), false));
      }
    }
  }

  public OffHeapServerStore(ResourcePageSource source, KeySegmentMapper mapper, boolean writeBehindConfigured) {
    this.mapper = mapper;
    segments = new ArrayList<>(mapper.getSegments());
    long maxSize = getMaxSize(source.getPool().getSize());
    for (int i = 0; i < mapper.getSegments(); i++) {
      if (writeBehindConfigured) {
        segments.add(new PinningOffHeapChainMap<>(source, LongPortability.INSTANCE, KILOBYTES.toBytes(4), (int) KILOBYTES.toBytes(maxSize), false));
      } else {
        segments.add(new OffHeapChainMap<>(source, LongPortability.INSTANCE, KILOBYTES.toBytes(4), (int)KILOBYTES.toBytes(maxSize), false));
      }
    }
  }

  public List<OffHeapChainMap<Long>> getSegments() {
    return segments;
  }

  static long getMaxSize(long poolSize) {
    long l = Long.highestOneBit(poolSize);
    long sizeInKb = KILOBYTES.convert(l, BYTES);
    long maxSize = sizeInKb >> 5;

    if (maxSize >= MAX_PAGE_SIZE_IN_KB) {
      maxSize = MAX_PAGE_SIZE_IN_KB;
    }
    return maxSize;
  }

  public void setEventListener(ServerStoreEventListener listener) {
    if (this.listener != null) {
      throw new IllegalStateException("ServerStoreEventListener instance already set");
    }
    this.listener = listener;
    OffHeapChainMap.ChainMapEvictionListener<Long> chainMapEvictionListener = listener::onEviction;
    for (OffHeapChainMap<Long> segment : segments) {
      segment.setEvictionListener(chainMapEvictionListener);
    }
  }

  @Override
  public Chain get(long key) {
    return segmentFor(key).get(key);
  }

  @Override
  public void append(long key, ByteBuffer payLoad) {
    LongConsumer lambda;
    if (listener != null && fireEvents) {
      lambda = (k) -> {
        Chain beforeAppend = segmentFor(k).getAndAppend(k, payLoad);
        listener.onAppend(beforeAppend, payLoad.duplicate());
      };
    } else {
      lambda = (k) -> segmentFor(k).append(k, payLoad);
    }

    try {
      lambda.accept(key);
    } catch (OversizeMappingException e) {
      consumeOversizeMappingException(key, lambda);
    }
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
    LongFunction<Chain> lambda;
    if (listener != null && fireEvents) {
      lambda = (k) -> {
        Chain beforeAppend = segmentFor(k).getAndAppend(k, payLoad);
        listener.onAppend(beforeAppend, payLoad.duplicate());
        return beforeAppend;
      };
    } else {
      lambda = (k) -> segmentFor(k).getAndAppend(k, payLoad);
    }

    try {
      return lambda.apply(key);
    } catch (OversizeMappingException e) {
      return handleOversizeMappingException(key, lambda);
    }
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    try {
      segmentFor(key).replaceAtHead(key, expect, update);
    } catch (OversizeMappingException e) {
      consumeOversizeMappingException(key, (long k) -> segmentFor(k).replaceAtHead(k, expect, update));
    }
  }

  public void put(long key, Chain chain) {
    try {
      try {segmentFor(key).put(key, chain);
    } catch (OversizeMappingException e) {
      consumeOversizeMappingException(key, (long k) -> segmentFor(k).put(k, chain));}
    } catch (Throwable t) {
      segmentFor(key).remove(key);
      throw t;
    }
  }

  public void remove(long key) {
    segmentFor(key).remove(key);
  }

  @Override
  public void clear() {
    for (OffHeapChainMap<Long> segment : segments) {
      segment.clear();
    }
  }

  OffHeapChainMap<Long> segmentFor(long key) {
    return segments.get(mapper.getSegmentForKey(key));
  }

  private void writeLockAll() {
    for (OffHeapChainMap<Long> s : segments) {
      s.writeLock().lock();
    }
  }

  private void writeUnlockAll() {
    for (OffHeapChainMap<Long> s : segments) {
      s.writeLock().unlock();
    }
  }

  private void consumeOversizeMappingException(long key, LongConsumer operation) {
    handleOversizeMappingException(key, k -> {
      operation.accept(k);
      return null;
    });
  }

  /**
   * Force eviction from other segments until {@code operation} succeeds or no further eviction is possible.
   *
   * @param key the target key
   * @param operation the previously failed operation
   * @param <R> operation result type
   * @return the operation result
   * @throws OversizeMappingException if the operation cannot be made to succeed
   */
  private <R> R handleOversizeMappingException(long key, LongFunction<R> operation) throws OversizeMappingException {
    if (tryShrinkOthers(key)) {
      try {
        return operation.apply(key);
      } catch (OversizeMappingException ex) {
        //ignore
      }
    }

    writeLockAll();
    try {
      OversizeMappingException e;
      do {
        try {
          return operation.apply(key);
        } catch (OversizeMappingException ex) {
          e = ex;
        }
      } while (tryShrinkOthers(key));
      throw e;
    } finally {
      writeUnlockAll();
    }
  }

  boolean tryShrinkOthers(long key) {
    boolean evicted = false;

    OffHeapChainMap<Long> target = segmentFor(key);
    for (OffHeapChainMap<Long> s : segments) {
      if (s != target) {
        evicted |= s.shrink();
      }
    }

    return evicted;
  }

  public void close() {
    writeLockAll();
    try {
      clear();
    } finally {
      writeUnlockAll();
    }
    segments.clear();
  }

  // stats

  @Override
  public long getAllocatedMemory() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getAllocatedMemory();
    }
    return total;
  }

  @Override
  public long getOccupiedMemory() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getOccupiedMemory();
    }
    return total;
  }

  @Override
  public long getDataAllocatedMemory() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getDataAllocatedMemory();
    }
    return total;
  }

  @Override
  public long getDataOccupiedMemory() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getDataOccupiedMemory();
    }
    return total;
  }

  @Override
  public long getDataSize() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getDataSize();
    }
    return total;
  }

  @Override
  public long getSize() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getSize();
    }
    return total;
  }

  @Override
  public long getTableCapacity() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getTableCapacity();
    }
    return total;
  }

  @Override
  public long getUsedSlotCount() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getUsedSlotCount();
    }
    return total;
  }

  @Override
  public long getRemovedSlotCount() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getRemovedSlotCount();
    }
    return total;
  }

  @Override
  public int getReprobeLength() {
    int total = 0;
    for (MapInternals segment : segments) {
      total += segment.getReprobeLength();
    }
    return total;
  }

  @Override
  public long getVitalMemory() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getVitalMemory();
    }
    return total;
  }

  @Override
  public long getDataVitalMemory() {
    long total = 0L;
    for (MapInternals segment : segments) {
      total += segment.getDataVitalMemory();
    }
    return total;
  }

  @Override
  public Iterator<Chain> iterator() {
    return new AggregateIterator<Chain>() {
      @Override
      protected Iterator<Chain> getNextIterator() {
        return listIterator.next().iterator();
      }
    };
  }

  public void enableEvents(boolean enable) {
    this.fireEvents = enable;
  }

  protected abstract class AggregateIterator<T> implements Iterator<T> {

    protected final Iterator<OffHeapChainMap<Long>> listIterator;
    protected Iterator<T>               currentIterator;

    protected abstract Iterator<T> getNextIterator();

    public AggregateIterator() {
      listIterator = segments.iterator();
      while (listIterator.hasNext()) {
        currentIterator = getNextIterator();
        if (currentIterator.hasNext()) {
          return;
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (currentIterator == null) {
        return false;
      }

      if (currentIterator.hasNext()) {
        return true;
      } else {
        while (listIterator.hasNext()) {
          currentIterator = getNextIterator();
          if (currentIterator.hasNext()) {
            return true;
          }
        }
        return false;
      }
    }

    @Override
    public T next() {
      if (currentIterator == null) {
        throw new NoSuchElementException();
      }

      if (currentIterator.hasNext()) {
        return currentIterator.next();
      } else {
        while (listIterator.hasNext()) {
          currentIterator = getNextIterator();

          if (currentIterator.hasNext()) {
            return currentIterator.next();
          }
        }
      }

      throw new NoSuchElementException();
    }

    @Override
    public void remove() {
      currentIterator.remove();
    }
  }
}
