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

package org.ehcache.core.store;

import org.ehcache.config.ResourceType;
import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.core.spi.store.WrapperStore;
import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceConfiguration;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Defines methods supporting working with {@link Store} implementations.
 */
public final class StoreSupport {
  /**
   * Private, niladic constructor to prevent instantiation.
   */
  private StoreSupport() {
  }

  public static Store.Provider selectWrapperStoreProvider(ServiceProvider<Service> serviceProvider, Collection<ServiceConfiguration<?>> serviceConfigs) {
    Collection<WrapperStore.Provider> storeProviders = serviceProvider.getServicesOfType(WrapperStore.Provider.class);
    Optional<Tuple<Integer, WrapperStore.Provider>> wrapperProvider = storeProviders.stream()
            .map(provider -> new Tuple<>(provider.wrapperStoreRank(serviceConfigs), provider))
            .filter(providerTuple -> providerTuple.x != 0)
            .max(Comparator.comparingInt(value -> value.x));
    return wrapperProvider.map(providerTuple -> providerTuple.y).orElse(null);
  }

  private static class Tuple<X, Y> {
    final X x;
    final Y y;
    Tuple(X x, Y y) {
      this.x = x;
      this.y = y;
    }
  }

  /**
   * Chooses a {@link org.ehcache.core.spi.store.Store.Provider Store.Provider} from those
   * available through the {@link ServiceLocator} that best supports the resource types and
   * service configurations provided.  This method relies on the
   * {@link Store.Provider#rank(Set, Collection) Store.Provider.rank} method in making the
   * selection.
   *
   * @param serviceProvider the {@code ServiceProvider} instance to use
   * @param resourceTypes the set of {@code ResourceType}s that must be supported by the provider
   * @param serviceConfigs the collection of {@code ServiceConfiguration}s used to influence the
   *                       selection
   *
   * @return the non-{@code null} {@code Store.Provider} implementation chosen
   *
   * @throws IllegalStateException if no suitable {@code Store.Provider} is available or if
   *        multiple {@code Store.Provider} implementations return the same top ranking
   */
  public static Store.Provider selectStoreProvider(
      ServiceProvider<Service> serviceProvider, Set<ResourceType<?>> resourceTypes, Collection<ServiceConfiguration<?>> serviceConfigs) {

    Collection<Store.Provider> storeProviders = serviceProvider.getServicesOfType(Store.Provider.class);
    List<Store.Provider> filteredStoreProviders = storeProviders.stream().filter(provider -> !(provider instanceof WrapperStore.Provider)).collect(Collectors.toList());
    int highRank = 0;
    List<Store.Provider> rankingProviders = new ArrayList<>();
    for (Store.Provider provider : filteredStoreProviders) {
      int rank = provider.rank(resourceTypes, serviceConfigs);
      if (rank > highRank) {
        highRank = rank;
        rankingProviders.clear();
        rankingProviders.add(provider);
      } else if (rank != 0 && rank == highRank) {
        rankingProviders.add(provider);
      }
    }

    if (rankingProviders.isEmpty()) {
      StringBuilder sb = new StringBuilder("No Store.Provider found to handle configured resource types ");
      sb.append(resourceTypes);
      sb.append(" from ");
      formatStoreProviders(filteredStoreProviders, sb);
      throw new IllegalStateException(sb.toString());
    } else if (rankingProviders.size() > 1) {
      StringBuilder sb = new StringBuilder("Multiple Store.Providers found to handle configured resource types ");
      sb.append(resourceTypes);
      sb.append(": ");
      formatStoreProviders(rankingProviders, sb);
      throw new IllegalStateException(sb.toString());
    }

    return rankingProviders.get(0);
  }

  private static void formatStoreProviders(final Collection<Store.Provider> storeProviders, final StringBuilder sb) {
    sb.append('{');
    boolean prependSeparator = false;
    for (final Store.Provider provider : storeProviders) {
      if (prependSeparator) {
        sb.append(", ");
      } else {
        prependSeparator = true;
      }
      sb.append(provider.getClass().getName());
    }
    sb.append('}');
  }
}
