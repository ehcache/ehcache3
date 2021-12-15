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
package org.ehcache.clustered.server.state;

import org.ehcache.clustered.common.ServerSideConfiguration;
import org.terracotta.offheapstore.buffersource.OffHeapBufferSource;
import org.terracotta.offheapstore.paging.OffHeapStorageArea;
import org.terracotta.offheapstore.paging.Page;
import org.terracotta.offheapstore.paging.PageSource;
import org.terracotta.offheapstore.paging.UpfrontAllocatingPageSource;

import static org.terracotta.offheapstore.util.MemoryUnit.GIGABYTES;
import static org.terracotta.offheapstore.util.MemoryUnit.MEGABYTES;

/**
 * Pairs a {@link ServerSideConfiguration.Pool} and an {@link UpfrontAllocatingPageSource} instance providing storage
 * for the pool.
 */
public class ResourcePageSource implements PageSource {
  /**
   * A description of the resource allocation underlying this {@code PageSource}.
   */
  private final ServerSideConfiguration.Pool pool;
  private final UpfrontAllocatingPageSource delegatePageSource;

  public ResourcePageSource(ServerSideConfiguration.Pool pool) {
    this.pool = pool;
    this.delegatePageSource = new UpfrontAllocatingPageSource(new OffHeapBufferSource(), pool.getSize(), GIGABYTES.toBytes(1), MEGABYTES.toBytes(128));
  }

  public ServerSideConfiguration.Pool getPool() {
    return pool;
  }

  public long getAllocatedSize() {
    return delegatePageSource.getAllocatedSizeUnSync();
  }

  @Override
  public Page allocate(int size, boolean thief, boolean victim, OffHeapStorageArea owner) {
    return delegatePageSource.allocate(size, thief, victim, owner);
  }

  @Override
  public void free(Page page) {
    delegatePageSource.free(page);
  }

  @Override
  public String toString() {
    String sb = "ResourcePageSource{" + "pool=" + pool +
                ", delegatePageSource=" + delegatePageSource +
                '}';
    return sb;
  }
}
