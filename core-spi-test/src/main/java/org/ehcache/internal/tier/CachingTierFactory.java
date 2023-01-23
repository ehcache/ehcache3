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

package org.ehcache.internal.tier;

import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.tiering.CachingTier;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Aurelien Broszniowski
 */
public interface CachingTierFactory<K, V> {

  CachingTier<K, V> newCachingTier();

  CachingTier<K, V> newCachingTier(long capacity);

  Store.ValueHolder<V> newValueHolder(V value);

  Store.Provider newProvider();

  Class<K> getKeyType();

  Class<V> getValueType();

  ServiceConfiguration<?, ?>[] getServiceConfigurations();

  ServiceProvider<Service> getServiceProvider();

  K createKey(long seed);

  V createValue(long seed);

  void disposeOf(CachingTier<K, V> tier);
}
