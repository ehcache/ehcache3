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
package org.ehcache.impl.internal.store.loaderwriter;

import org.ehcache.core.spi.store.Store;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;

public class LocalWriteBehindLoaderWriterStore<K, V> extends LocalLoaderWriterStore<K, V> {

  private final CacheLoaderWriter<? super K, V> cacheLoaderWriter;

  public LocalWriteBehindLoaderWriterStore(Store<K, V> delegate, CacheLoaderWriter<? super K, V> cacheLoaderWriter, boolean useLoaderInAtomics, ExpiryPolicy<? super K, ? super V> expiry) {
    super(delegate, cacheLoaderWriter, useLoaderInAtomics, expiry);
    this.cacheLoaderWriter = cacheLoaderWriter;
  }

  public CacheLoaderWriter<?, ?> getCacheLoaderWriter() {
    return cacheLoaderWriter;
  }
}
