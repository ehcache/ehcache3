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

package org.ehcache.transactions.xa.internal.journal;

import org.ehcache.CachePersistenceException;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.core.spi.service.FileBasedPersistenceContext;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.spi.service.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ludovic Orban
 */
public class DefaultJournalProvider implements JournalProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultJournalProvider.class);

  private volatile LocalPersistenceService persistenceService;

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    this.persistenceService = serviceProvider.getService(LocalPersistenceService.class);
  }

  @Override
  public void stop() {
    this.persistenceService = null;
  }

  @Override
  public <K> Journal<K> getJournal(LocalPersistenceService.PersistenceSpaceIdentifier persistentSpaceId, Serializer<K> keySerializer) {
    if (persistentSpaceId == null) {
      LOGGER.info("Using transient XAStore journal");
      return new TransientJournal<K>();
    }

    try {
      LOGGER.info("Using persistent XAStore journal");
      FileBasedPersistenceContext persistenceContext = persistenceService.createPersistenceContextWithin(persistentSpaceId, "XAJournal");
      return new PersistentJournal<K>(persistenceContext.getDirectory(), keySerializer);
    } catch (CachePersistenceException cpe) {
      throw new RuntimeException(cpe);
    }
  }
}
