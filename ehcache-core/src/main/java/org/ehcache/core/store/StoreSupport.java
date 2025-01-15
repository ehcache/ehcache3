/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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

import org.ehcache.core.spi.ServiceLocator;
import org.ehcache.core.spi.store.Store;
import org.ehcache.spi.service.ServiceProvider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.ToIntFunction;
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

  public static <T> T select(Class<T> serviceType, ServiceProvider<? super T> serviceProvider, ToIntFunction<T> ranking) {
    return trySelect(serviceType, serviceProvider, ranking).orElseThrow(() -> {
      Class<?> enclosingClass = serviceType.getEnclosingClass();
      String type = enclosingClass == null ? serviceType.getSimpleName() : enclosingClass.getSimpleName() + "." + serviceType.getSimpleName();
      StringBuilder sb = new StringBuilder("No " + type + " types found to handle configuration ");
      sb.append(" from ");
      sb.append(serviceProvider.getServicesOfType(serviceType).stream().map(p -> p.getClass().getName())
        .collect(Collectors.joining(", ", "{", "}")));
      return new IllegalStateException(sb.toString());
    });
  }

  /**
   * Chooses a {@link ServiceProvider} from those
   * available through the {@link ServiceLocator} according to the supplied ranking.
   *
   * @param serviceProvider the {@code ServiceProvider} instance to use
   * @param ranking ranking function
   *
   * @return the non-{@code null} {@code ServiceProvider} implementation chosen
   *
   * @throws IllegalStateException if multiple {@code Store.Provider} implementations return the same top ranking
   */
  public static <T> Optional<T> trySelect(Class<T> serviceType, ServiceProvider<? super T> serviceProvider, ToIntFunction<T> ranking) {
    Collection<T> providers = serviceProvider.getServicesOfType(serviceType);
    int highRank = 0;
    List<T> rankingProviders = new ArrayList<>();
    for (T provider : providers) {
      int rank = ranking.applyAsInt(provider);
      if (rank > highRank) {
        highRank = rank;
        rankingProviders.clear();
        rankingProviders.add(provider);
      } else if (rank != 0 && rank == highRank) {
        rankingProviders.add(provider);
      }
    }

    if (rankingProviders.isEmpty()) {
      return Optional.empty();
    } else if (rankingProviders.size() > 1) {
      Class<?> enclosingClass = serviceType.getEnclosingClass();
      String type = enclosingClass == null ? serviceType.getSimpleName() : enclosingClass.getSimpleName() + "." + serviceType.getSimpleName();
      StringBuilder sb = new StringBuilder("Multiple " + type + " types found to handle configuration ");
      sb.append(": ");
      sb.append(rankingProviders.stream().map(p -> p.getClass().getName())
        .collect(Collectors.joining(", ", "{", "}")));
      throw new IllegalStateException(sb.toString());
    } else {
      return Optional.of(rankingProviders.get(0));
    }
  }
}
