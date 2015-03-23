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
package org.ehcache.internal.store.tiering;

import org.ehcache.spi.cache.tiering.AuthoritativeTier;
import org.ehcache.spi.cache.tiering.CachingTier;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Ludovic Orban
 */
public class CacheStoreServiceConfig implements ServiceConfiguration<CacheStore.Provider> {

  private Class<? extends CachingTier.Provider> cachingTierProvider;
  private Class<? extends AuthoritativeTier.Provider> authoritativeTierProvider;

  public CacheStoreServiceConfig cachingTierProvider(Class<? extends CachingTier.Provider> cachingTierProvider) {
    this.cachingTierProvider = cachingTierProvider;
    return this;
  }

  public CacheStoreServiceConfig authoritativeTierProvider(Class<? extends AuthoritativeTier.Provider> authoritativeTierProvider) {
    this.authoritativeTierProvider = authoritativeTierProvider;
    return this;
  }

  public Class<? extends CachingTier.Provider> cachingTierProvider() {
    return cachingTierProvider;
  }

  public Class<? extends AuthoritativeTier.Provider> authoritativeTierProvider() {
    return authoritativeTierProvider;
  }

  @Override
  public Class<CacheStore.Provider> getServiceType() {
    return CacheStore.Provider.class;
  }
}
