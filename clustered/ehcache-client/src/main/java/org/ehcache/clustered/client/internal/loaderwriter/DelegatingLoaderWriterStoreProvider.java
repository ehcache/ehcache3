/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
package org.ehcache.clustered.client.internal.loaderwriter;

import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.client.service.ClusteringService.ClusteredCacheIdentifier;
import org.ehcache.core.spi.store.AbstractWrapperStoreProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;

@ServiceDependencies({CacheLoaderWriterProvider.class, ClusteringService.class})
public class DelegatingLoaderWriterStoreProvider extends AbstractWrapperStoreProvider {

  @SuppressWarnings({"rawtypes", "unchecked"})
  public DelegatingLoaderWriterStoreProvider() {
    super((Class) CacheLoaderWriterConfiguration.class, ClusteredCacheIdentifier.class);
  }

  @Override
  protected int wrapperRank() {
    return 2;
  }

  @Override
  protected int wrapperPriority() {
    return super.wrapperPriority() + 1;
  }

  @Override
  protected <K, V> Store<K, V> wrap(Store<K, V> store, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    return store;
  }
}
