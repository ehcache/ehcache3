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

package org.ehcache.clustered.server;

import org.ehcache.clustered.common.internal.ServerStoreConfiguration;
import org.ehcache.clustered.common.internal.store.Chain;
import org.ehcache.clustered.server.offheap.OffHeapChainMap;
import org.ehcache.clustered.server.offheap.OffHeapServerStore;
import org.ehcache.clustered.server.state.ResourcePageSource;
import org.terracotta.offheapstore.MapInternals;
import org.terracotta.offheapstore.exceptions.OversizeMappingException;
import org.terracotta.offheapstore.paging.PageSource;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class ServerStoreImpl implements ServerSideServerStore, MapInternals {

  private final ServerStoreConfiguration storeConfiguration;
  private final ResourcePageSource pageSource;
  private final OffHeapServerStore store;

  public ServerStoreImpl(ServerStoreConfiguration configuration, ResourcePageSource source, KeySegmentMapper mapper,
                         List<OffHeapChainMap<Long>> recoveredMaps) {
    this.storeConfiguration = configuration;
    this.pageSource = source;
    this.store = new OffHeapServerStore(recoveredMaps, mapper);
  }

  public ServerStoreImpl(ServerStoreConfiguration storeConfiguration, ResourcePageSource pageSource, KeySegmentMapper mapper, boolean writeBehindConfigured) {
    this.storeConfiguration = storeConfiguration;
    this.pageSource = pageSource;
    this.store = new OffHeapServerStore(pageSource, mapper, writeBehindConfigured);
  }

  @Override
  public void setEventListener(ServerStoreEventListener listener) {
    store.setEventListener(listener);
  }

  @Override
  public void enableEvents(boolean enable) {
    store.enableEvents(enable);
  }

  /**
   * Gets the {@link PageSource} providing storage for this {@code ServerStore}.
   *
   * @return the {@code PageSource} used by this {@code ServerStore}
   */
  public PageSource getPageSource() {
    return pageSource;
  }

  public ServerStoreConfiguration getStoreConfiguration() {
    return storeConfiguration;
  }

  @Override
  public Chain get(long key) {
    return store.get(key);
  }

  @Override
  public void append(long key, ByteBuffer payLoad) {
    checkPayLoadSize(payLoad);
    store.append(key, payLoad);
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
    checkPayLoadSize(payLoad);
    return store.getAndAppend(key, payLoad);
  }

  @Override
  public void replaceAtHead(long key, Chain expect, Chain update) {
    store.replaceAtHead(key, expect, update);
  }

  public void put(long key, Chain chain) {
    store.put(key, chain);
  }

  @Override
  public void remove(long key) {
    store.remove(key);
  }

  @Override
  public void clear() {
    store.clear();
  }

  public void close() {
    store.close();
  }

  @Override
  public List<Set<Long>> getSegmentKeySets() {

    return new AbstractList<Set<Long>>() {
      @Override
      public Set<Long> get(int index) {
        return store.getSegments().get(index).keySet();
      }
      @Override
      public int size() {
        return store.getSegments().size();
      }
    };
  }

  // stats


  @Override
  public long getSize() {
    return store.getSize();
  }

  @Override
  public long getTableCapacity() {
    return store.getTableCapacity();
  }

  @Override
  public long getUsedSlotCount() {
    return store.getUsedSlotCount();
  }

  @Override
  public long getRemovedSlotCount() {
    return store.getRemovedSlotCount();
  }

  @Override
  public long getAllocatedMemory() {
    return store.getAllocatedMemory();
  }

  @Override
  public long getOccupiedMemory() {
    return store.getOccupiedMemory();
  }

  @Override
  public long getVitalMemory() {
    return store.getVitalMemory();
  }

  @Override
  public long getDataAllocatedMemory() {
    return store.getDataAllocatedMemory();
  }

  @Override
  public long getDataOccupiedMemory() {
    return store.getDataOccupiedMemory();
  }

  @Override
  public long getDataVitalMemory() {
    return store.getDataVitalMemory();
  }

  @Override
  public long getDataSize() {
    return store.getDataSize();
  }

  @Override
  public int getReprobeLength() {
    //TODO
    //MapInternals Interface may need to change to implement this function correctly.
    //Currently MapInternals Interface contains function: int getReprobeLength();
    //however OffHeapServerStore.reprobeLength() returns a long
    //Thus there could be data loss

    throw new UnsupportedOperationException("Not supported yet.");
  }

  private void checkPayLoadSize(ByteBuffer payLoad) {
    if (payLoad.remaining() > pageSource.getPool().getSize()) {
      throw new OversizeMappingException("Payload (" + payLoad.remaining() +
                                         ") bigger than pool size (" + pageSource.getPool().getSize() + ")");
    }
  }

  @Override
  public Iterator<Chain> iterator() {
    return store.iterator();
  }
}
