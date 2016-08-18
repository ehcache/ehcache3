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

import java.util.ArrayList;
import java.util.List;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * Implements the delete operation for write behind
 *
 * @author Geert Bevin
 * @author Tim wu
 *
 */
public class DeleteOperation<K, V> implements SingleOperation<K, V> {

  private final K key;
  private final long creationTime;

  /**
   * Create a new delete operation for a particular entry
   *
   */
  public DeleteOperation(K key) {
    this(key, System.nanoTime());
  }

  /**
   * Create a new delete operation for a particular entry and creation time
   *
   */
  public DeleteOperation(K key, long creationTime) {
    this.key = key;
    this.creationTime = creationTime;
  }

  public void performSingleOperation(CacheLoaderWriter<K, V> cacheLoaderWriter) throws Exception {
    cacheLoaderWriter.delete(key);
  }

  public BatchOperation<K, V> createBatchOperation(List<? extends SingleOperation<K, V>> operations) {
    final List<K> keys = new ArrayList<K>();
      for (KeyBasedOperation<K> operation : operations) {
        keys.add(operation.getKey());
      }
    return new DeleteAllOperation<K, V>(keys);
  }

  @Override
  public K getKey() {
    return this.key;
  }

  @Override
  public long getCreationTime() {
    return creationTime;
  }

  @Override
  public int hashCode() {
    return getKey().hashCode();
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DeleteOperation<?, ?>) {
      return getCreationTime() == ((DeleteOperation<K, V>) other).getCreationTime() && getKey().equals(((DeleteOperation<K, V>) other).getKey());
    } else {
      return false;
    }
  }

}
