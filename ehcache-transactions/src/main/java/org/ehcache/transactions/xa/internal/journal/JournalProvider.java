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

import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.Service;

/**
 * Provider of {@link Journal} implementations.
 *
 * @author Ludovic Orban
 */
public interface JournalProvider extends Service {

  /**
   * Get the {@link Journal} implementation.
   *
   * @param persistentSpaceId the ID of a persistent space, or null if the journal does not need to be persisted.
   * @param keySerializer the key serializer, or null if the journal does not need to be persisted.
   * @param <K> the store's key type.
   * @return a {@link Journal} implementation.
   */
  <K> Journal<K> getJournal(PersistableResourceService.PersistenceSpaceIdentifier<?> persistentSpaceId, Serializer<K> keySerializer);
}
