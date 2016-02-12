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
package org.ehcache.internal.sizeof;

import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.copy.IdentityCopier;
import org.ehcache.internal.sizeof.listeners.EhcacheVisitorListener;
import org.ehcache.internal.store.heap.holders.CopiedOnHeapKey;
import org.ehcache.sizeof.SizeOfFilterSource;
import org.ehcache.sizeof.SizeOf;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.sizeof.SizeOfEngine;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngine implements SizeOfEngine {

  private final long maxObjectGraphSize;
  private final long maxObjectSize;
  private final SizeOf sizeOf;
  private final long chmTreeBinOffset;
  private final long onHeapKeyOffset;
  private final SizeOfFilterSource filterSource = new SizeOfFilterSource(true);

  public DefaultSizeOfEngine(long maxObjectGraphSize, long maxObjectSize) {
    this.maxObjectGraphSize = maxObjectGraphSize;
    this.maxObjectSize = maxObjectSize;
    this.sizeOf = SizeOf.newInstance(filterSource.getFilters());
    this.onHeapKeyOffset = sizeOf.deepSizeOf(new CopiedOnHeapKey(new Object(), new IdentityCopier()));
    this.chmTreeBinOffset = sizeOf.deepSizeOf(ConcurrentHashMap.FAKE_TREE_BIN);
  }

  @Override
  public <K, V> long sizeof(K key, Store.ValueHolder<V> holder) {
    return sizeOf.deepSizeOf(new EhcacheVisitorListener(maxObjectGraphSize, maxObjectSize), key, holder) + this.chmTreeBinOffset + this.onHeapKeyOffset;
  }

}
