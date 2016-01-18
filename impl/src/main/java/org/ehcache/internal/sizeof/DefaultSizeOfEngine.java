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

import java.io.Serializable;

import org.ehcache.internal.concurrent.ConcurrentHashMap;
import org.ehcache.internal.copy.IdentityCopier;
import org.ehcache.internal.serialization.CompactJavaSerializer;
import org.ehcache.internal.sizeof.listeners.EhcacheVisitorListener;
import org.ehcache.internal.store.heap.holders.CopiedOnHeapKey;
import org.ehcache.internal.store.heap.holders.CopiedOnHeapValueHolder;
import org.ehcache.internal.store.heap.holders.SerializedOnHeapValueHolder;
import org.ehcache.sizeof.EhcacheFilterSource;
import org.ehcache.sizeof.SizeOf;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.sizeof.SizeOfEngine;

/**
 * @author Abhilash
 *
 */
public class DefaultSizeOfEngine implements SizeOfEngine {
  
  private final long maxDepth;
  private final long maxSize;
  private final SizeOf sizeOf;
  private final EhcacheFilterSource filterSource = new EhcacheFilterSource(true);
  
  private final long chmTreeBinOffset; 
  private final long onHeapValueHolderOffset;
  private final long onHeapKeyOffset;
  
  public DefaultSizeOfEngine(long maxDepth, long maxSize) {
    this(maxDepth, maxSize, false);
  }
  
  public DefaultSizeOfEngine(long maxDepth, long maxSize, boolean isValueSerialized) {
    this.maxDepth = maxDepth;
    this.maxSize = maxSize;
    this.filterSource.ignoreInstancesOf(Copier.class, false);
    this.filterSource.ignoreInstancesOf(Serializer.class, false);
    this.sizeOf = SizeOf.newInstance(filterSource.getFilters());
    
    this.onHeapKeyOffset = sizeof(new CopiedOnHeapKey(new Object(), new IdentityCopier()));
    if (isValueSerialized) {
      this.onHeapValueHolderOffset = sizeof(new SerializedOnHeapValueHolder(new AnySerializable(), 0l, false, new CompactJavaSerializer(null)));
    } else {
      this.onHeapValueHolderOffset = sizeof(new CopiedOnHeapValueHolder(new Object(), 0l, false, new IdentityCopier()));
    }
    
    this.chmTreeBinOffset = sizeof(ConcurrentHashMap.FAKE_TREE_BIN);
  }

  @Override
  public long sizeof(Object... objects) {    
    return sizeOf.deepSizeOf(new EhcacheVisitorListener(maxDepth, maxSize), objects);
  }

  @Override
  public long sizeofKey(Object key) {
    return sizeof(key) + this.onHeapKeyOffset;
  }

  @Override
  public long sizeofValue(Object value) {
    return sizeof(value) + this.onHeapValueHolderOffset;
  }

  @Override
  public long getCHMOffset() {
    return this.chmTreeBinOffset;
  }
  
  private static class AnySerializable implements Serializable {
    
  }

}
