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

package org.ehcache.core.spi.store;

import org.ehcache.spi.service.PluralService;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;

/**
 * Marker interface for {@link Store}s which act like wrapper and does not have any storage, rather
 * delegate the storage to other stores
 * @param <K> the key type
 * @param <V> the value type
 */
public interface WrapperStore<K, V> extends Store<K, V> {

  /**
   * Service to create {@link WrapperStore}s
   */
  @PluralService
  interface Provider extends Store.Provider {

    /**
     * Gets the internal ranking for the {@code WrapperStore} instances provided by this {@code Provider} of the wrapper
     * store's
     *
     * @param serviceConfigs the collection of {@code ServiceConfiguration} instances that may contribute
     *                       to the ranking
     * @return a non-negative rank indicating the ability of a {@code WrapperStore} created by this {@code Provider}
     */
    int wrapperStoreRank(Collection<ServiceConfiguration<?, ?>> serviceConfigs);

  }
}
