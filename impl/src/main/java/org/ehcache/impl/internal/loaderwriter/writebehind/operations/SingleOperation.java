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

import java.util.List;

import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

/**
 * @author Geert Bevin
 * @author Chris Dennis
 *
 */
public interface SingleOperation<K, V> extends KeyBasedOperation<K> {

  /**
   * Perform this operation as a single execution with the provided cache writer
   *
   */
  void performSingleOperation(CacheLoaderWriter<K, V> cacheLoaderWriter) throws Exception;

  /**
   * Creates a batch operation that corresponds to the operation type of this single operation.
   * <p/>
   * This batch operation will not be stored in the queue anymore and is solely used for structuring.
   * The data from the single operation will already be processed in the final form that will be expected by the
   * {@code CacheWriter} that will be used to execute the batch operation.
   *
   */
  BatchOperation<K, V> createBatchOperation(List<? extends SingleOperation<K, V>> operations);
}
