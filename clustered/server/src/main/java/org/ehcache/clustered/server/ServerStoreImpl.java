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
import org.ehcache.clustered.server.EhcacheStateServiceImpl.ResourcePageSource;
import org.ehcache.clustered.server.offheap.OffHeapServerStore;
import org.terracotta.offheapstore.paging.PageSource;

import com.tc.classloader.CommonComponent;

import java.nio.ByteBuffer;

@CommonComponent
public class ServerStoreImpl implements ServerStore {

  private static final int OFFHEAP_CHAIN_SEGMENTS = 16;

  private final ServerStoreConfiguration storeConfiguration;
  private final ResourcePageSource pageSource;
  private final OffHeapServerStore store;

  public ServerStoreImpl(ServerStoreConfiguration storeConfiguration, ResourcePageSource pageSource) {
    this.storeConfiguration = storeConfiguration;
    this.pageSource = pageSource;
    this.store = new OffHeapServerStore(pageSource, OFFHEAP_CHAIN_SEGMENTS);
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

  @Override
  public void clear() {
    store.clear();
  }

  public void close() {
    store.close();
  }
}
