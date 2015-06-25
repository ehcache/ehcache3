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

package org.ehcache.config.persistence;

import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.expiry.Expiry;
import org.ehcache.spi.cache.Store;

/**
 * @author Alex Snaps
 */
public class PersistentStoreConfigurationImpl<K, V> implements Store.PersistentStoreConfiguration<K, V, String> {

  private final Store.Configuration<K, V> config;
  private final String identifier;

  public PersistentStoreConfigurationImpl(Store.Configuration<K, V> config, String identifier) {
    this.config = config;
    this.identifier = identifier;
  }

  @Override
  public String getIdentifier() {
    return identifier;
  }

  @Override
  public boolean isPersistent() {
    return config.getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent();
  }

  @Override
  public Class<K> getKeyType() {
    return config.getKeyType();
  }

  @Override
  public Class<V> getValueType() {
    return config.getValueType();
  }

  @Override
  public EvictionVeto<? super K, ? super V> getEvictionVeto() {
    return config.getEvictionVeto();
  }

  @Override
  public EvictionPrioritizer<? super K, ? super V> getEvictionPrioritizer() {
    return config.getEvictionPrioritizer();
  }

  @Override
  public ClassLoader getClassLoader() {
    return config.getClassLoader();
  }

  @Override
  public Expiry<? super K, ? super V> getExpiry() {
    return config.getExpiry();
  }

  @Override
  public ResourcePools getResourcePools() {
    return config.getResourcePools();
  }
}
