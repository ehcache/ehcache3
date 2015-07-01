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

package org.ehcache;

import org.ehcache.config.ResourceType;
import org.ehcache.events.CacheEventNotificationService;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.LocalPersistenceService;
import org.slf4j.LoggerFactory;

/**
 * PersistentUserManagedEhcache
 */
public class PersistentUserManagedEhcache<K, V> extends Ehcache<K, V> implements PersistentUserManagedCache<K, V> {

  private final Store.PersistentStoreConfiguration storeConfig;
  private final LocalPersistenceService localPersistenceService;
  private final String id;

  public PersistentUserManagedEhcache(RuntimeConfiguration<K, V> runtimeConfiguration, Store<K, V> store, Store.PersistentStoreConfiguration storeConfig, LocalPersistenceService localPersistenceService, CacheLoaderWriter<? super K, V> cacheLoaderWriter, CacheEventNotificationService<K, V> eventNotifier, String id) {
    super(runtimeConfiguration, store, cacheLoaderWriter, eventNotifier, true, LoggerFactory.getLogger(PersistentUserManagedEhcache.class.getName() + "-" + id));
    this.storeConfig = storeConfig;
    this.localPersistenceService = localPersistenceService;
    this.id = id;

  }

  @Override
  public Maintainable toMaintenance() {
    final StatusTransitioner.Transition st = internalToMaintenance();
    try {
      final Maintainable maintainable = new Maintainable() {
        @Override
        public void create() {
          PersistentUserManagedEhcache.this.create();
        }

        @Override
        public void destroy() {
          PersistentUserManagedEhcache.this.destroy();
        }

        @Override
        public void close() {
          internalExitMaintenance().succeeded();
        }
      };
      st.succeeded();
      return maintainable;
    } catch (RuntimeException e) {
      st.failed(e); // this throws
    }
    throw new AssertionError("Should not reach this line... ever!");
  }

  void create() {
    checkMaintenance();
    try {
      localPersistenceService.createPersistenceContext(id, storeConfig);
    } catch (CachePersistenceException e) {
      throw new RuntimeException("Unable to create persistence context for user managed cache " + id, e);
    }
  }

  void destroy() {
    checkMaintenance();
    try {
      localPersistenceService.destroyPersistenceContext(id);
    } catch (CachePersistenceException e) {
      throw new RuntimeException("Could not destroy persistent storage for user managed cache " + id, e);
    }
  }

  @Override
  public void close() {
    super.close();
    if (!getRuntimeConfiguration().getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent()) {
      try {
        localPersistenceService.destroyPersistenceContext(id);
      } catch (CachePersistenceException e) {
        logger.debug("Unable to clear persistent context", e);
      }
    }
  }
}
