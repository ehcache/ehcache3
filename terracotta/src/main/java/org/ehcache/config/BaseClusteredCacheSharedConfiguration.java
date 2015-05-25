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

package org.ehcache.config;

import org.ehcache.expiry.Expiry;

/**
 * @author Alex Snaps
 */
public class BaseClusteredCacheSharedConfiguration<K, V> implements ClusteredCacheSharedConfiguration<K, V> {

  private final Class<K> kClass;
  private final Class<V> vClass;
  private final EvictionVeto<? super K, ? super V> evictionVeto;
  private final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer;
  private final Expiry<? super K, ? super V> expiry;
  private final boolean transactionalCache;
  private final CacheLoaderWriter cacheLoaderWriter;
  private final ResourcePool resourcePool;

  public BaseClusteredCacheSharedConfiguration(final Class<K> kClass,
                                               final Class<V> vClass) {
    this(kClass, vClass, null, null, null); // todo: accepts whatever config is on the server side?
  }

  public BaseClusteredCacheSharedConfiguration(final Class<K> kClass,
                                               final Class<V> vClass,
                                               final EvictionVeto<? super K, ? super V> evictionVeto,
                                               final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer,
                                               final Expiry<? super K, ? super V> expiry) {

    this(kClass, vClass, evictionVeto, evictionPrioritizer, expiry, null, false, CacheLoaderWriter.NONE);
  }

  @Deprecated
  public BaseClusteredCacheSharedConfiguration(final Class<K> kClass,
                                               final Class<V> vClass,
                                               final EvictionVeto<? super K, ? super V> evictionVeto,
                                               final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer,
                                               final Expiry<? super K, ? super V> expiry,
                                               final long size,
                                               final ResourceUnit unit,
                                               final boolean persistent,
                                               final boolean transactionalCache,
                                               final CacheLoaderWriter cacheLoaderWriter) {
    this(kClass, vClass, evictionVeto, evictionPrioritizer, expiry, null, transactionalCache, cacheLoaderWriter);

  }

  public BaseClusteredCacheSharedConfiguration(final Class<K> kClass,
                                               final Class<V> vClass,
                                               final EvictionVeto<? super K, ? super V> evictionVeto,
                                               final EvictionPrioritizer<? super K, ? super V> evictionPrioritizer,
                                               final Expiry<? super K, ? super V> expiry,
                                               final ResourcePool resourcePool,
                                               final boolean transactionalCache,
                                               final CacheLoaderWriter cacheLoaderWriter) {
    this.kClass = kClass;
    this.vClass = vClass;
    this.evictionVeto = evictionVeto;
    this.evictionPrioritizer = evictionPrioritizer;
    this.expiry = expiry;
    this.resourcePool = resourcePool;
    this.transactionalCache = transactionalCache;
    this.cacheLoaderWriter = cacheLoaderWriter;
  }

  @Override
  public Class<K> getKeyType() {
    return kClass;
  }

  @Override
  public Class<V> getValueType() {
    return vClass;
  }

  @Override
  public EvictionVeto<? super K, ? super V> getEvictionVeto() {
    return evictionVeto;
  }

  @Override
  public EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer() {
    return evictionPrioritizer;
  }

  @Override
  public Expiry<? super K, ? super V> getExpiry() {
    return expiry;
  }

  @Override
  public boolean isTransactional() {
    return transactionalCache;
  }

  @Override
  public boolean isWriteThrough() {
    return cacheLoaderWriter == CacheLoaderWriter.REQUIRED;
  }

  @Override
  public boolean isWriteBehind() {
    return cacheLoaderWriter == CacheLoaderWriter.LOADER_REQUIRED;
  }

  @Override
  public ResourcePool getResourcePool() {
    return resourcePool;
  }

  public enum CacheLoaderWriter {
    NONE,
    LOADER_REQUIRED,
    REQUIRED,
  }
}
