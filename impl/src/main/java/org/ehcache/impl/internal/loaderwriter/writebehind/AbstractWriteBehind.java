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
package org.ehcache.impl.internal.loaderwriter.writebehind;

import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.DeleteOperation;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.SingleOperation;
import org.ehcache.impl.internal.loaderwriter.writebehind.operations.WriteOperation;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

abstract class AbstractWriteBehind<K, V> implements WriteBehind<K, V> {

  private final CacheLoaderWriter<K, V> cacheLoaderWriter;

  public AbstractWriteBehind(CacheLoaderWriter<K, V> cacheLoaderWriter) {
    this.cacheLoaderWriter = cacheLoaderWriter;
  }

  @Override
  public V load(K key) throws Exception {
    SingleOperation<K, V> operation = getOperation(key);
    return operation == null ? cacheLoaderWriter.load(key) : (operation.getClass() == WriteOperation.class ? ((WriteOperation<K, V>) operation).getValue() : null);
  }

  @Override
  public void write(K key, V value) throws CacheWritingException {
    addOperation(new WriteOperation<>(key, value));
  }

  @Override
  public void delete(K key) throws CacheWritingException {
    addOperation(new DeleteOperation<>(key));
  }

  protected abstract SingleOperation<K, V> getOperation(K key);

  protected abstract void addOperation(final SingleOperation<K, V> operation);
}
