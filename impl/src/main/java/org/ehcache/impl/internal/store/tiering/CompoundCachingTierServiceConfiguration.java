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
package org.ehcache.impl.internal.store.tiering;

import org.ehcache.core.spi.store.tiering.HigherCachingTier;
import org.ehcache.core.spi.store.tiering.LowerCachingTier;
import org.ehcache.spi.service.ServiceConfiguration;

/**
 * @author Ludovic Orban
 */
public class CompoundCachingTierServiceConfiguration implements ServiceConfiguration<CompoundCachingTier.Provider> {

  private Class<? extends HigherCachingTier.Provider> higherProvider;
  private Class<? extends LowerCachingTier.Provider> lowerProvider;

  public Class<? extends HigherCachingTier.Provider> higherProvider() {
    return higherProvider;
  }

  public CompoundCachingTierServiceConfiguration higherProvider(Class<? extends HigherCachingTier.Provider> higherProvider) {
    this.higherProvider = higherProvider;
    return this;
  }

  public Class<? extends LowerCachingTier.Provider> lowerProvider() {
    return lowerProvider;
  }

  public CompoundCachingTierServiceConfiguration lowerProvider(Class<? extends LowerCachingTier.Provider> lowerProvider) {
    this.lowerProvider = lowerProvider;
    return this;
  }

  @Override
  public Class<CompoundCachingTier.Provider> getServiceType() {
    return CompoundCachingTier.Provider.class;
  }
}
