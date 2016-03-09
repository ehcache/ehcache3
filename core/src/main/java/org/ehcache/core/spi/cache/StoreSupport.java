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

package org.ehcache.core.spi.cache;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.Collection;
import java.util.Set;

/**
 * Defines methods supporting working with {@link Store} implementations.
 *
 * @author Clifford W. Johnson
 */
public final class StoreSupport {
  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private StoreSupport() {
  }

  /**
   * Chooses a {@link org.ehcache.core.spi.cache.Store.Provider Store.Provider} from those
   * available through the {@link ServiceLocator} that best supports the resource types and
   * service configurations provided.  This method relies on the
   * {@link Store.Provider#rank(Set, Collection) Store.Provider.rank} method in making the
   * selection.
   *
   * @param serviceLocator the {@code ServiceLocator} instance to use
   * @param resourceTypes the set of {@code ResourceType}s that must be supported by the provider
   * @param serviceConfigs the collection of {@code ServiceConfiguration}s used to influence the
   *                       selection
   *
   * @return the non-{@code null} {@code Store.Provider} implementation chosen
   *
   * @throws IllegalStateException if no suitable {@code Store.Provider} is available
   */
  public static Store.Provider selectStoreProvider(
      final ServiceLocator serviceLocator, final Set<ResourceType> resourceTypes, final Collection<ServiceConfiguration<?>> serviceConfigs) {
    final Collection<Store.Provider> storeProviders = serviceLocator.getServicesOfType(Store.Provider.class);
    if (storeProviders.isEmpty()) {
      throw new IllegalStateException("No Store.Provider available");
    }
    int highRank = 0;
    Store.Provider rankingProvider = null;
    for (final Store.Provider provider : storeProviders) {
      int rank = provider.rank(resourceTypes, serviceConfigs);
      if (rank > highRank) {
        highRank = rank;
        rankingProvider = provider;
      }
    }
    if (rankingProvider == null) {
      throw new IllegalStateException("No Store.Provider found to handle configured resource types: " + resourceTypes);
    }
    return rankingProvider;
  }
}
