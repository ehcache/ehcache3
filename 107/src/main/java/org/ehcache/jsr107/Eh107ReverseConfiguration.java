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

import java.io.ObjectStreamException;

import javax.cache.configuration.CacheEntryListenerConfiguration;

/**
 * Eh107ReverseConfiguration
 */
class Eh107ReverseConfiguration<K, V> extends Eh107Configuration<K, V> {

  private static final long serialVersionUID = 7690458739466020356L;

  private final transient Cache<K, V> cache;
  private final boolean readThrough;
  private final boolean writeThrough;
  private final boolean storeByValueOnHeap;
  private boolean managementEnabled = false;
  private boolean statisticsEnabled = true;

  Eh107ReverseConfiguration(Cache<K, V> cache, boolean readThrough, boolean writeThrough, boolean storeByValueOnHeap) {
    this.cache = cache;
    this.readThrough = readThrough;
    this.writeThrough = writeThrough;
    this.storeByValueOnHeap = storeByValueOnHeap;
  }

  @Override
  public boolean isReadThrough() {
    return readThrough;
  }

  @Override
  public boolean isWriteThrough() {
    return writeThrough;
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
    // Nothing to do - we do not maintain / expose them
  }

  @Override
  public void removeCacheEntryListenerConfiguration(CacheEntryListenerConfiguration<K, V> cacheEntryListenerConfiguration) {
    // Nothing to do - we do not maintain / expose them
  }

  @Override
  public <T> T unwrap(Class<T> clazz) {
      return Unwrap.unwrap(clazz, this, cache.getRuntimeConfiguration());
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
    return storeByValueOnHeap;
  }

  private Object writeReplace() throws ObjectStreamException {
    throw new UnsupportedOperationException("Serialization of Ehcache provider configuration classes is not supported");
  }
}
