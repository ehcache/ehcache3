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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.common.internal.store.ServerStore;
import org.ehcache.clustered.server.KeySegmentMapper;
import org.ehcache.clustered.server.ServerStoreEvictionListener;
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.terracotta.offheapstore.MapInternals;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;

import static org.terracotta.offheapstore.util.MemoryUnit.BYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.KILOBYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

public class OffHeapServerStore implements ServerStore, MapInternals {

  private static final long MAX_PAGE_SIZE_IN_KB = KILOBYTES.convert(8, MEGABYTES);

  private final List<OffHeapChainMap<Long>> segments;
  private final KeySegmentMapper mapper;

  OffHeapServerStore(PageSource source, KeySegmentMapper mapper) {
    this.mapper = mapper;
    segments = new ArrayList<OffHeapChainMap<Long>>(mapper.getSegments());
    for (int i = 0; i < mapper.getSegments(); i++) {
      segments.add(new OffHeapChainMap<Long>(source, LongPortability.INSTANCE, KILOBYTES.toBytes(4), MEGABYTES.toBytes(8), false));
    }
  }

  public OffHeapServerStore(ResourcePageSource source, KeySegmentMapper mapper) {
    this.mapper = mapper;
    segments = new ArrayList<OffHeapChainMap<Long>>(mapper.getSegments());
    long maxSize = getMaxSize(source.getPool().getSize());
    for (int i = 0; i < mapper.getSegments(); i++) {
      segments.add(new OffHeapChainMap<Long>(source, LongPortability.INSTANCE, KILOBYTES.toBytes(4), (int) KILOBYTES.toBytes(maxSize), false));
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

  public void setEvictionListener(final ServerStoreEvictionListener listener) {
    OffHeapChainMap.ChainMapEvictionListener<Long> chainMapEvictionListener = new OffHeapChainMap.ChainMapEvictionListener<Long>() {
      @Override
      public void onEviction(Long key) {
        listener.onEviction(key);
      }
    };
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
    try {
      segmentFor(key).append(key, payLoad);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key)) {
        try {
          segmentFor(key).append(key, payLoad);
          return;
        } catch (OversizeMappingException ex) {
          //ignore
        }
      }

      writeLockAll();
      try {
        do {
          try {
            segmentFor(key).append(key, payLoad);
            return;
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
    try {
      return segmentFor(key).getAndAppend(key, payLoad);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key)) {
        try {
          return segmentFor(key).getAndAppend(key, payLoad);
        } catch (OversizeMappingException ex) {
          //ignore
        }
      }

      writeLockAll();
      try {
        do {
          try {
            return segmentFor(key).getAndAppend(key, payLoad);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    try {
      segmentFor(key).replaceAtHead(key, expect, update);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key)) {
        try {
          segmentFor(key).replaceAtHead(key, expect, update);
          return;
        } catch (OversizeMappingException ex) {
          //ignore
        }
      }

      writeLockAll();
      try {
        do {
          try {
            segmentFor(key).replaceAtHead(key, expect, update);
            return;
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
  }

  public void put(long key, Chain chain) {
    try {
      segmentFor(key).put(key, chain);
    } catch (OversizeMappingException e) {
      if (handleOversizeMappingException(key)) {
        try {
          segmentFor(key).put(key, chain);
        } catch (OversizeMappingException ex) {
          //ignore
        }
      }

      writeLockAll();
      try {
        do {
          try {
            segmentFor(key).put(key, chain);
          } catch (OversizeMappingException ex) {
            e = ex;
          }
        } while (handleOversizeMappingException(key));
        throw e;
      } finally {
        writeUnlockAll();
      }
    }
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

  boolean handleOversizeMappingException(long hash) {
    boolean evicted = false;

    OffHeapChainMap<Long> target = segmentFor(hash);
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

}
