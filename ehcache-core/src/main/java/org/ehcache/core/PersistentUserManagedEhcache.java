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

package org.ehcache.core;

import org.ehcache.PersistentUserManagedCache;
import org.ehcache.Status;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.core.events.CacheEventDispatcher;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.loaderwriter.BulkCacheLoadingException;
import org.ehcache.spi.loaderwriter.BulkCacheWritingException;
import org.ehcache.spi.loaderwriter.CacheLoadingException;
import org.ehcache.CachePersistenceException;
import org.ehcache.spi.loaderwriter.CacheWritingException;
import org.ehcache.core.spi.LifeCycled;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.persistence.PersistableResourceService;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.slf4j.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Implementation of {@link PersistentUserManagedCache} which is a cache with a persistent resource outside of a
 * {@link org.ehcache.CacheManager}.
 * <p>
 * {@code Ehcache} users should not have to depend on this type but rely exclusively on the api types in package
 * {@code org.ehcache}.
 */
public class PersistentUserManagedEhcache<K, V> implements PersistentUserManagedCache<K, V> {

  private final Logger logger;
  private final Ehcache<K,V> cache;
  private final PersistableResourceService persistableResourceService;
  private final String id;

  /**
   * Creates a new {@code PersistentUserManagedCache} based on the provided parameters.
   *
   * @param configuration the cache configuration
   * @param store the underlying store
   * @param persistableResourceService the persistence service
   * @param cacheLoaderWriter the optional loader writer
   * @param eventDispatcher the event dispatcher
   * @param id an id for this cache
   */
  public PersistentUserManagedEhcache(CacheConfiguration<K, V> configuration, Store<K, V> store, ResilienceStrategy<K, V> resilienceStrategy, PersistableResourceService persistableResourceService, CacheLoaderWriter<? super K, V> cacheLoaderWriter, CacheEventDispatcher<K, V> eventDispatcher, String id) {
    this.logger = EhcachePrefixLoggerFactory.getLogger(PersistentUserManagedEhcache.class);
    this.cache = new Ehcache<>(new EhcacheRuntimeConfiguration<>(configuration), store, resilienceStrategy, eventDispatcher, cacheLoaderWriter);
    this.persistableResourceService = persistableResourceService;
    this.id = id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void destroy() throws CachePersistenceException {
    StatusTransitioner.Transition st = cache.statusTransitioner.maintenance();
    try {
      st.succeeded();
    } catch (Throwable t) {
      throw st.failed(t);
    }
    destroyInternal();
    // Exit maintenance mode once #934 is solved
//    statusTransitioner.exitMaintenance().succeeded();
  }

  void destroyInternal() throws CachePersistenceException {
    cache.statusTransitioner.checkMaintenance();
    persistableResourceService.destroy(id);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void init() {
    cache.init();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    cache.close();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Status getStatus() {
    return cache.statusTransitioner.currentStatus();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V get(K key) throws CacheLoadingException {
    return cache.get(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void put(K key, V value) throws CacheWritingException {
    cache.put(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean containsKey(K key) {
    return cache.containsKey(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void remove(K key) throws CacheWritingException {
    cache.remove(key);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<K, V> getAll(Set<? extends K> keys) throws BulkCacheLoadingException {
    return cache.getAll(keys);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void putAll(Map<? extends K, ? extends V> entries) throws BulkCacheWritingException {
    cache.putAll(entries);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void removeAll(Set<? extends K> keys) throws BulkCacheWritingException {
    cache.removeAll(keys);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void clear() {
    cache.clear();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V putIfAbsent(K key, V value) throws CacheLoadingException, CacheWritingException {
    return cache.putIfAbsent(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean remove(K key, V value) throws CacheWritingException {
    return cache.remove(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public V replace(K key, V value) throws CacheLoadingException, CacheWritingException {
    return cache.replace(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean replace(K key, V oldValue, V newValue) throws CacheLoadingException, CacheWritingException {
    return cache.replace(key, oldValue, newValue);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public CacheRuntimeConfiguration<K, V> getRuntimeConfiguration() {
    return cache.getRuntimeConfiguration();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterator<Entry<K, V>> iterator() {
    return cache.iterator();
  }

  /**
   * Adds a hook to lifecycle transitions.
   */
  public void addHook(LifeCycled lifeCycled) {
    cache.statusTransitioner.addHook(lifeCycled);
  }
}
