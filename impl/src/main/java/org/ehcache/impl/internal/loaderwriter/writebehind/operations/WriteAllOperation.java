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
package org.ehcache.impl.internal.loaderwriter.writebehind.operations;

import java.util.Map;

import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * Implements the write all operation for write behind
 *
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public class WriteAllOperation<K, V> implements BatchOperation<K, V> {

  private final Iterable<? extends Map.Entry<? extends K, ? extends V>> entries;

  /**
   * Create a new write all operation for the provided list of element
   *
   */
  public WriteAllOperation(Iterable<? extends Map.Entry<? extends K, ? extends V>> entries) {
    this.entries = entries;
  }

  public void performOperation(CacheLoaderWriter<K, V> cacheLoaderWriter) throws BulkCacheWritingException, Exception {
    cacheLoaderWriter.writeAll(entries);
  }

}
