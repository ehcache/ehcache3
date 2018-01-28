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
package org.ehcache.impl.internal.spi.resilience;

import org.ehcache.core.internal.resilience.RobustLoaderWriterResilienceStrategy;
import org.ehcache.core.internal.resilience.RobustResilienceStrategy;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.RecoveryStore;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.resilience.ResilienceStrategyProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

public class DefaultResilienceStrategyProvider implements ResilienceStrategyProvider {
  @Override
  public <K, V> ResilienceStrategy<K, V> createResilienceStrategy(String alias, RecoveryStore<K> recoveryStore) {
    return new RobustResilienceStrategy<>(recoveryStore);
  }

  @Override
  public <K, V> ResilienceStrategy<K, V> createResilienceStrategy(String alias, RecoveryStore<K> recoveryStore, CacheLoaderWriter<? super K, V> loaderWriter) {
    return new RobustLoaderWriterResilienceStrategy<>(recoveryStore, loaderWriter);
  }

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {
    //no-op
  }

  @Override
  public void stop() {
    //no-op
  }
}
