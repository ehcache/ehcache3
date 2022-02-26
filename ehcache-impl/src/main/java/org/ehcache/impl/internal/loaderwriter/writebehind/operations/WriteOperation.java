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

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * Implements the write operation for write behind
 *
 * @author Geert Bevin
 * @author Tim wu
 *
 */

public class WriteOperation<K, V> implements SingleOperation<K, V> {

  private final K key;
  private final V value;
  private final long creationTime;

  /**
   * Create a new write operation for a particular element
   *
   */
  public WriteOperation(K k, V v) {
    this(k, v, System.nanoTime());
  }

  /**
   * Create a new write operation for a particular element and creation time
   *
   */
  public WriteOperation(K k, V v, long creationTime) {
    this.key = k;
    this.value = v;
    this.creationTime = creationTime;
  }

  @Override
  public void performOperation(CacheLoaderWriter<K, V> cacheWriter) throws Exception {
    cacheWriter.write(key, value);
  }

  @Override
  public K getKey() {
    return this.key;
  }

  public V getValue(){
    return this.value;
  }

  @Override
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public int hashCode() {
    int hash = (int) getCreationTime();
    hash = hash * 31 + getKey().hashCode();
    return hash;
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof WriteOperation) {
      return getCreationTime() == ((WriteOperation<?, ?>) other).getCreationTime() && getKey().equals(((WriteOperation<?, ?>) other).getKey());
    } else {
      return false;
    }
  }

}
