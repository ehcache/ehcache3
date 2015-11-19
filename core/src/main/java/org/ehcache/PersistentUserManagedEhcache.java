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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.ResourceType;
import org.ehcache.events.CacheEventDispatcher;
import org.ehcache.exceptions.BulkCacheLoadingException;
import org.ehcache.exceptions.BulkCacheWritingException;
import org.ehcache.exceptions.CacheLoadingException;
import org.ehcache.exceptions.CachePersistenceException;
import org.ehcache.exceptions.CacheWritingException;
import org.ehcache.spi.LifeCycled;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.LocalPersistenceService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * PersistentUserManagedEhcache
 */
public class PersistentUserManagedEhcache<K, V> implements PersistentUserManagedCache<K, V> {

  private final StatusTransitioner statusTransitioner;
  private final Logger logger;
  private final Ehcache<K,V> ehcache;
  private final LocalPersistenceService localPersistenceService;
  private final String id;

  public PersistentUserManagedEhcache(CacheConfiguration<K, V> configuration, Store<K, V> store, Store.Configuration<K, V> storeConfig, LocalPersistenceService localPersistenceService, CacheLoaderWriter<? super K, V> cacheLoaderWriter, CacheEventDispatcher<K, V> eventNotifier, String id) {
    this.logger = LoggerFactory.getLogger(PersistentUserManagedEhcache.class.getName() + "-" + id);
    this.statusTransitioner = new StatusTransitioner(logger);
    this.ehcache = new Ehcache<K, V>(new EhcacheRuntimeConfiguration<K, V>(configuration, eventNotifier), store, cacheLoaderWriter, eventNotifier, true, logger, statusTransitioner);
    this.localPersistenceService = localPersistenceService;
    this.id = id;
  }

  @Override
  public Maintainable toMaintenance() {
    final StatusTransitioner.Transition st = statusTransitioner.maintenance();
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
          statusTransitioner.exitMaintenance().succeeded();
        }
      };
      st.succeeded();
      return maintainable;
    } catch (RuntimeException e) {
      throw st.failed(e);
    }
  }

  void create() {
    statusTransitioner.checkMaintenance();
    try {
      if (!getRuntimeConfiguration().getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent()) {
        destroy();
      }
      localPersistenceService.getOrCreatePersistenceSpace(id);
    } catch (CachePersistenceException e) {
      throw new RuntimeException("Unable to create persistence space for user managed cache " + id, e);
    }
  }

  void destroy() {
    statusTransitioner.checkMaintenance();
    try {
      localPersistenceService.destroyPersistenceSpace(id);
    } catch (CachePersistenceException e) {
      throw new RuntimeException("Could not destroy persistence space for user managed cache " + id, e);
    }
  }

  @Override
  public void init() {
    ehcache.init();
  }

  @Override
  public void close() {
    ehcache.close();
    if (!getRuntimeConfiguration().getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent()) {
      try {
        localPersistenceService.destroyPersistenceSpace(id);
      } catch (CachePersistenceException e) {
        logger.debug("Unable to clear persistence space for user managed cache " + id, e);
      }
    }
  }

  @Override
  public Status getStatus() {
    return statusTransitioner.currentStatus();
  }

  @Override
  public V get(K key) throws CacheLoadingException {
    return ehcache.get(key);
  }

  @Override
  public void put(K key, V value) throws CacheWritingException {
    ehcache.put(key, value);
  }

  @Override
  public boolean containsKey(K key) {
    return ehcache.containsKey(key);
  }

  @Override
  public void remove(K key) throws CacheWritingException {
    ehcache.remove(key);
  }

  @Override
  public Map<K, V> getAll(Set<? extends K> keys) throws BulkCacheLoadingException {
    return ehcache.getAll(keys);
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
    ehcache.putAll(entries);
  }

  @Override
  public void removeAll(Set<? extends K> keys) throws BulkCacheWritingException {
    ehcache.removeAll(keys);
  }

  @Override
  public void clear() {
    ehcache.clear();
  }

  @Override
  public V putIfAbsent(K key, V value) throws CacheLoadingException, CacheWritingException {
    return ehcache.putIfAbsent(key, value);
  }

  @Override
  public boolean remove(K key, V value) throws CacheWritingException {
    return ehcache.remove(key, value);
  }

  @Override
  public V replace(K key, V value) throws CacheLoadingException, CacheWritingException {
    return ehcache.replace(key, value);
  }

  @Override
  public boolean replace(K key, V oldValue, V newValue) throws CacheLoadingException, CacheWritingException {
    return ehcache.replace(key, oldValue, newValue);
  }

  @Override
  public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
    return ehcache.getRuntimeConfiguration();
  }

  @Override
  public Iterator<Entry<K, V>> iterator() {
    return ehcache.iterator();
  }

  void addHook(LifeCycled lifeCycled) {
    statusTransitioner.addHook(lifeCycled);
  }
}
