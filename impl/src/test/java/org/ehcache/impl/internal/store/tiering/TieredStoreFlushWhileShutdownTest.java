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

package org.ehcache.impl.internal.store.tiering;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePools;
import org.ehcache.core.spi.service.DiskResourceService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.persistence.DefaultDiskResourceService;
import org.ehcache.impl.internal.store.disk.OffHeapDiskStore;
import org.ehcache.impl.internal.store.heap.OnHeapStore;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.ehcache.impl.serialization.JavaSerializer;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.persistence.PersistableResourceService.PersistenceSpaceIdentifier;
import org.ehcache.spi.service.ServiceConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.Serializable;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.internal.service.ServiceLocator.dependencySet;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TieredStoreFlushWhileShutdownTest {

  @Rule
  public final TemporaryFolder folder = new TemporaryFolder();

  @Test
  public void testTieredStoreReleaseFlushesEntries() throws Exception {
    File persistenceLocation = folder.newFolder("testTieredStoreReleaseFlushesEntries");

    Store.Configuration<Number, String> configuration = new Store.Configuration<Number, String>() {

      @Override
      public Class<Number> getKeyType() {
        return Number.class;
      }

      @Override
      public Class<String> getValueType() {
        return String.class;
      }

      @Override
      public EvictionAdvisor<? super Number, ? super String> getEvictionAdvisor() {
        return null;
      }

      @Override
      public ClassLoader getClassLoader() {
        return getClass().getClassLoader();
      }

      @Override
      public Expiry<? super Number, ? super String> getExpiry() {
        return Expirations.noExpiration();
      }

      @Override
      public ResourcePools getResourcePools() {
        return newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).disk(10, MemoryUnit.MB, true).build();
      }

      @Override
      public Serializer<Number> getKeySerializer() {
        return new JavaSerializer<Number>(getClassLoader());
      }

      @Override
      public Serializer<String> getValueSerializer() {
        return new JavaSerializer<String>(getClassLoader());
      }

      @Override
      public int getDispatcherConcurrency() {
        return 1;
      }
    };

    ServiceLocator serviceLocator = getServiceLocator(persistenceLocation);
    serviceLocator.startAllServices();
    TieredStore.Provider tieredStoreProvider = new TieredStore.Provider();

    tieredStoreProvider.start(serviceLocator);

    CacheConfiguration cacheConfiguration = mock(CacheConfiguration.class);
    when(cacheConfiguration.getResourcePools()).thenReturn(newResourcePoolsBuilder().disk(1, MemoryUnit.MB, true).build());

    DiskResourceService diskResourceService = serviceLocator.getService(DiskResourceService.class);
    PersistenceSpaceIdentifier persistenceSpace = diskResourceService.getPersistenceSpaceIdentifier("testTieredStoreReleaseFlushesEntries", cacheConfiguration);
    Store<Number, String> tieredStore = tieredStoreProvider.createStore(configuration, persistenceSpace);
    tieredStoreProvider.initStore(tieredStore);
    for (int i = 0; i < 100; i++) {
      tieredStore.put(i, "hello");
    }

    for(int j = 0; j < 20; j++){
      for (int i = 0; i < 20; i++) {
        tieredStore.get(i);
      }
    }

    tieredStoreProvider.releaseStore(tieredStore);
    tieredStoreProvider.stop();

    serviceLocator.stopAllServices();

    ServiceLocator serviceLocator1 = getServiceLocator(persistenceLocation);
    serviceLocator1.startAllServices();
    tieredStoreProvider.start(serviceLocator1);

    DiskResourceService diskResourceService1 = serviceLocator1.getService(DiskResourceService.class);
    PersistenceSpaceIdentifier persistenceSpace1 = diskResourceService1.getPersistenceSpaceIdentifier("testTieredStoreReleaseFlushesEntries", cacheConfiguration);
    tieredStore = tieredStoreProvider.createStore(configuration, persistenceSpace1);
    tieredStoreProvider.initStore(tieredStore);

    for(int i = 0; i < 20; i++) {
      assertThat(tieredStore.get(i).hits(), is(21L));
    }
  }

  private ServiceLocator getServiceLocator(File location) throws Exception {
    DefaultPersistenceConfiguration persistenceConfiguration = new DefaultPersistenceConfiguration(location);
    DefaultLocalPersistenceService fileService = new DefaultLocalPersistenceService(persistenceConfiguration);
    DefaultDiskResourceService diskResourceService = new DefaultDiskResourceService();
    ServiceLocator.DependencySet dependencySet = dependencySet();
    dependencySet.with(fileService);
    dependencySet.with(diskResourceService);
    dependencySet.with(new OnHeapStore.Provider());
    dependencySet.with(new OffHeapDiskStore.Provider());
    return dependencySet.build();
  }
}
