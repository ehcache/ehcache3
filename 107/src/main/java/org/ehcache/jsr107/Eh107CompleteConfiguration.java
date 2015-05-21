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
package org.ehcache.jsr107;

import org.ehcache.config.CacheConfiguration;

import java.io.ObjectStreamException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

/**
 * @author teck
 */
class Eh107CompleteConfiguration<K, V> extends Eh107Configuration<K, V> implements CompleteConfiguration<K, V> {

  private static final long serialVersionUID = -142083640934760400L;

  private final Class<K> keyType;
  private final Class<V> valueType;
  private final boolean isStoreByValue;
  private final boolean isReadThrough;
  private final boolean isWriteThrough;
  private volatile boolean isStatisticsEnabled;
  private volatile boolean isManagementEnabled;
  private final List<CacheEntryListenerConfiguration<K, V>> cacheEntryListenerConfigs = new CopyOnWriteArrayList<CacheEntryListenerConfiguration<K, V>>();
  private final Factory<CacheLoader<K, V>> cacheLoaderFactory;
  private final Factory<CacheWriter<? super K, ? super V>> cacheWriterFactory;
  private final Factory<ExpiryPolicy> expiryPolicyFactory;

  private final transient CacheConfiguration<K, V> ehcacheConfig;


  Eh107CompleteConfiguration(Configuration<K, V> config) {
    this(config, null);
  }

  Eh107CompleteConfiguration(Configuration<K, V> config, org.ehcache.config.CacheConfiguration<K, V> ehcacheConfig) {
    this.ehcacheConfig = ehcacheConfig;
    this.keyType = config.getKeyType();
    this.valueType = config.getValueType();
    this.isStoreByValue = config.isStoreByValue();

    if (config instanceof CompleteConfiguration) {
      CompleteConfiguration<K, V> completeConfig = (CompleteConfiguration<K, V>) config;
      this.isReadThrough = completeConfig.isReadThrough();
      this.isWriteThrough = completeConfig.isWriteThrough();
      this.isStatisticsEnabled = completeConfig.isStatisticsEnabled();
      this.isManagementEnabled = completeConfig.isManagementEnabled();
      this.cacheLoaderFactory = completeConfig.getCacheLoaderFactory();
      this.cacheWriterFactory = completeConfig.getCacheWriterFactory();
      this.expiryPolicyFactory = completeConfig.getExpiryPolicyFactory();
      for (CacheEntryListenerConfiguration<K, V> listenerConfig : completeConfig.getCacheEntryListenerConfigurations()) {
        cacheEntryListenerConfigs.add(listenerConfig);
      }
    } else {
      this.isReadThrough = false;
      this.isWriteThrough = false;
      this.isStatisticsEnabled = false;
      this.isManagementEnabled = false;
      this.cacheLoaderFactory = null;
      this.cacheWriterFactory = null;
      this.expiryPolicyFactory = EternalExpiryPolicy.factoryOf();
    }
  }

  @Override
  public Class<K> getKeyType() {
    return this.keyType;
  }

  @Override
  public Class<V> getValueType() {
    return this.valueType;
  }

  @Override
  public boolean isStoreByValue() {
    return this.isStoreByValue;
  }

  @Override
  public boolean isReadThrough() {
    return this.isReadThrough;
  }

  @Override
  public boolean isWriteThrough() {
    return this.isWriteThrough;
  }

  @Override
  public boolean isStatisticsEnabled() {
    return this.isStatisticsEnabled;
  }

  @Override
  public boolean isManagementEnabled() {
    return this.isManagementEnabled;
  }

  @Override
  public Iterable<CacheEntryListenerConfiguration<K, V>> getCacheEntryListenerConfigurations() {
    return Collections.unmodifiableList(this.cacheEntryListenerConfigs);
  }

  @Override
  public Factory<CacheLoader<K, V>> getCacheLoaderFactory() {
    return this.cacheLoaderFactory;
  }

  @Override
  public Factory<CacheWriter<? super K, ? super V>> getCacheWriterFactory() {
    return this.cacheWriterFactory;
  }

  @Override
  public Factory<ExpiryPolicy> getExpiryPolicyFactory() {
    return this.expiryPolicyFactory;
  }

  @Override
  void setManagementEnabled(boolean isManagementEnabled) {
    this.isManagementEnabled = isManagementEnabled;
  }

  @Override
  void setStatisticsEnabled(boolean isStatisticsEnabled) {
    this.isStatisticsEnabled = isStatisticsEnabled;
  }

  @Override
  void addCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> listenerConfig) {
    this.cacheEntryListenerConfigs.add(listenerConfig);
  }

  @Override
  void removeCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> listenerConfig) {
    this.cacheEntryListenerConfigs.remove(listenerConfig);
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
    try {
      return Unwrap.unwrap(clazz, this);
    } catch (IllegalArgumentException e) {
      return Unwrap.unwrap(clazz, ehcacheConfig);
    }
  }

  private Object writeReplace() throws ObjectStreamException {
    throw new UnsupportedOperationException("Serialization of Ehcache provider configuration classes is not supported");
  }
}
