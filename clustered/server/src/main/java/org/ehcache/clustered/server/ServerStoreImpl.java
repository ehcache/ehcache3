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
import org.ehcache.clustered.common.internal.store.ServerStore;
import org.ehcache.clustered.server.offheap.OffHeapChainMap;
import org.ehcache.clustered.server.offheap.OffHeapServerStore;
import org.terracotta.offheapstore.MapInternals;
import org.terracotta.offheapstore.paging.PageSource;

import com.tc.classloader.CommonComponent;

import java.nio.ByteBuffer;
import java.util.List;

@CommonComponent
public class ServerStoreImpl implements ServerSideServerStore {

  private final ServerStoreConfiguration storeConfiguration;
  private final PageSource pageSource;
  private final OffHeapServerStore store;

  public ServerStoreImpl(ServerStoreConfiguration storeConfiguration, PageSource pageSource, KeySegmentMapper mapper) {
    this.storeConfiguration = storeConfiguration;
    this.pageSource = pageSource;
    this.store = new OffHeapServerStore(pageSource, mapper);
  }

  public void setEvictionListener(ServerStoreEvictionListener listener) {
    store.setEvictionListener(listener);
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
    store.append(key, payLoad);
  }

  @Override
  public Chain getAndAppend(long key, ByteBuffer payLoad) {
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
  public void clear() {
    store.clear();
  }

  public void close() {
    store.close();
  }

  public List<OffHeapChainMap<Long>> getSegments() {
    return store.getSegments();
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
}
