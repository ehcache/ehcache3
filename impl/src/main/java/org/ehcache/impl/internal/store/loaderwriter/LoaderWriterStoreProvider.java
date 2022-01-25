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

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.store.AbstractWrapperStoreProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriterProvider;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;

import java.util.Collection;
import java.util.Set;

import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

@ServiceDependencies({CacheLoaderWriterProvider.class, WriteBehindProvider.class})
public class LoaderWriterStoreProvider extends AbstractWrapperStoreProvider {

  private volatile WriteBehindProvider writeBehindProvider;

  @Override
  protected <K, V> Store<K, V> wrap(Store<K, V> store, Store.Configuration<K, V> storeConfig, ServiceConfiguration<?, ?>... serviceConfigs) {
    WriteBehindConfiguration<?> writeBehindConfiguration = findSingletonAmongst(WriteBehindConfiguration.class, (Object[]) serviceConfigs);
    LocalLoaderWriterStore<K, V> loaderWriterStore;
    if(writeBehindConfiguration == null) {
      loaderWriterStore = new LocalLoaderWriterStore<>(store, storeConfig.getCacheLoaderWriter(), storeConfig.useLoaderInAtomics(), storeConfig.getExpiry());
    } else {
      CacheLoaderWriter<? super K, V> writeBehindLoaderWriter = writeBehindProvider.createWriteBehindLoaderWriter(storeConfig.getCacheLoaderWriter(), writeBehindConfiguration);
      loaderWriterStore = new LocalWriteBehindLoaderWriterStore<>(store, writeBehindLoaderWriter, storeConfig.useLoaderInAtomics(), storeConfig.getExpiry());
    }
    return loaderWriterStore;
  }

  @Override
  public void releaseStore(Store<?, ?> resource) {
    try {
      if (resource instanceof LocalWriteBehindLoaderWriterStore<?, ?>) {
        writeBehindProvider.releaseWriteBehindLoaderWriter(((LocalWriteBehindLoaderWriterStore<?, ?>) resource).getCacheLoaderWriter());
      }
    } finally {
      super.releaseStore(resource);
    }
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    super.start(serviceProvider);
    this.writeBehindProvider = serviceProvider.getService(WriteBehindProvider.class);
  }

  @Override
  public void stop() {
    this.writeBehindProvider = null;
    super.stop();
  }

  @Override
  public int rank(Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    throw new UnsupportedOperationException("Its a Wrapper store provider, does not support regular ranking");
  }

  @Override
  public int wrapperStoreRank(Collection<ServiceConfiguration<?, ?>> serviceConfigs) {
    CacheLoaderWriterConfiguration<?> loaderWriterConfiguration = findSingletonAmongst(CacheLoaderWriterConfiguration.class, serviceConfigs);
    if (loaderWriterConfiguration == null) {
      return 0;
    }
    return 2;
  }
}
