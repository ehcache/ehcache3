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

import org.ehcache.internal.sizeof.listeners.EhcacheVisitorListener;
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
  
  public DefaultSizeOfEngine(long maxDepth, long maxSize) {
    this.maxDepth = maxDepth;
    this.maxSize = maxSize;
    this.filterSource.ignoreInstancesOf(Copier.class, false);
    this.filterSource.ignoreInstancesOf(Serializer.class, false);
    this.sizeOf = SizeOf.newInstance(filterSource.getFilters());
  }

  @Override
  public long sizeof(Object... objects) {    
    return sizeOf.deepSizeOf(new EhcacheVisitorListener(maxDepth, maxSize), objects);
  }

}
