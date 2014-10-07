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

package org.ehcache.internal.store;

import org.ehcache.Cache;
import org.ehcache.function.Predicate;
import org.ehcache.spi.cache.Store;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Comparator;

/**
 * @author Alex Snaps
 */
public interface StoreFactory<K, V> {

  Store<K, V> newStore(Store.Configuration<K, V> config);

  Store.ValueHolder<V> newValueHolder(V value);

  Store.Provider newProvider();

  Store.Configuration<K, V> newConfiguration(
      Class<K> keyType, Class<V> valueType, Comparable<Long> capacityConstraint,
      Predicate<Cache.Entry<K, V>> evictionVeto, Comparator<Cache.Entry<K, V>> evictionPrioritizer);

  Class<K> getKeyType();

  Class<V> getValueType();

  ServiceConfiguration[] getServiceConfigurations();
}
