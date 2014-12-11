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

import org.ehcache.Cache;

import javax.cache.configuration.CacheEntryListenerConfiguration;

/**
 * Eh107ReverseConfiguration
 */
class Eh107ReverseConfiguration<K, V> extends Eh107Configuration<K, V> {
  
  private static final long serialVersionUID = 7690458739466020356L;
  
  private final Cache<K, V> cache;
  private boolean managementEnabled = false;
  private boolean statisticsEnabled = false;

  Eh107ReverseConfiguration(Cache<K, V> cache) {
    this.cache = cache;
  }

  @Override
  public boolean isReadThrough() {
    // FIXME
    return false;
  }

  @Override
  public boolean isWriteThrough() {
    // FIXME
    return false;
  }

  @Override
  public boolean isStatisticsEnabled() {
    return statisticsEnabled;
  }

  @Override
  public void setStatisticsEnabled(boolean enabled) {
    this.statisticsEnabled = enabled;
  }

  @Override
  public boolean isManagementEnabled() {
    return managementEnabled;
  }

  @Override
  public void setManagementEnabled(boolean enabled) {
    this.managementEnabled = enabled;
  }

  @Override
  public void addCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    // FIXME: need some behavior here
  }

  @Override
  public void removeCacheEntryListenerConfiguration(
      CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    // FIXME: need some behavior here
  }

  @Override
  public Class<K> getKeyType() {
    return cache.getRuntimeConfiguration().getKeyType();
  }

  @Override
  public Class<V> getValueType() {
    return cache.getRuntimeConfiguration().getValueType();
  }

  @Override
  public boolean isStoreByValue() {
    // FIXME: need to find value from cache
    return false;
  }
}
