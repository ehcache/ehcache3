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

package org.ehcache.transactions.xa.internal;

import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.StatisticsService;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.time.TimeSourceService;
import org.ehcache.core.statistics.DefaultStatisticsService;
import org.ehcache.impl.internal.DefaultTimeSourceService;
import org.ehcache.impl.internal.store.offheap.OffHeapStore;
import org.ehcache.spi.persistence.StateRepository;
import org.ehcache.spi.serialization.StatefulSerializer;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
import org.ehcache.transactions.xa.internal.journal.Journal;
import org.ehcache.transactions.xa.internal.journal.JournalProvider;
import org.ehcache.transactions.xa.txmgr.TransactionManagerWrapper;
import org.ehcache.transactions.xa.txmgr.provider.TransactionManagerProvider;
import org.junit.Test;

import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * XAStoreProviderTest
 */
public class XAStoreProviderTest {

  @SuppressWarnings("unchecked")
  @Test
  public void testXAStoreProviderStatefulSerializer() {
    OffHeapStore.Provider underlyingStoreProvider = new OffHeapStore.Provider();

    JournalProvider journalProvider = mock(JournalProvider.class);
    when(journalProvider.getJournal(null, null)).thenReturn(mock(Journal.class));

    TransactionManagerProvider transactionManagerProvider = mock(TransactionManagerProvider.class);
    when(transactionManagerProvider.getTransactionManagerWrapper()).thenReturn(mock(TransactionManagerWrapper.class));

    ServiceProvider<Service> serviceProvider = mock(ServiceProvider.class);
    when(serviceProvider.getService(JournalProvider.class)).thenReturn(journalProvider);
    when(serviceProvider.getService(TimeSourceService.class)).thenReturn(new DefaultTimeSourceService(null));
    when(serviceProvider.getService(TransactionManagerProvider.class)).thenReturn(transactionManagerProvider);
    when(serviceProvider.getService(StatisticsService.class)).thenReturn(new DefaultStatisticsService());
    when(serviceProvider.getServicesOfType(Store.Provider.class)).thenReturn(Collections.singleton(underlyingStoreProvider));

    Store.Configuration<String, String> configuration = mock(Store.Configuration.class);
    when(configuration.getResourcePools()).thenReturn(ResourcePoolsBuilder.newResourcePoolsBuilder().offheap(1, MemoryUnit.MB).build());
    when(configuration.getDispatcherConcurrency()).thenReturn(1);
    StatefulSerializer<String> valueSerializer = mock(StatefulSerializer.class);
    when(configuration.getValueSerializer()).thenReturn(valueSerializer);

    underlyingStoreProvider.start(serviceProvider);

    XAStore.Provider provider = new XAStore.Provider();
    provider.start(serviceProvider);

    Store<String, String> store = provider.createStore(configuration, mock(XAStoreConfiguration.class));
    provider.initStore(store);

    verify(valueSerializer).init(any(StateRepository.class));
  }
}
