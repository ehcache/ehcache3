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

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyProviderConfiguration;
import org.ehcache.spi.resilience.ResilienceStrategyProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

@Component
public class DefaultResilienceStrategyProviderFactory implements ServiceFactory<ResilienceStrategyProvider> {
  @Override
  public ResilienceStrategyProvider create(ServiceCreationConfiguration<ResilienceStrategyProvider> configuration) {
    if (configuration == null) {
      return new DefaultResilienceStrategyProvider();
    } else if (configuration instanceof DefaultResilienceStrategyProviderConfiguration) {
      return new DefaultResilienceStrategyProvider((DefaultResilienceStrategyProviderConfiguration) configuration);
    } else {
      throw new IllegalArgumentException("Expected a configuration of type DefaultResilienceStrategyProviderConfiguration (or none) but got " + configuration
        .getClass()
        .getName());
    }
  }

  @Override
  public Class<ResilienceStrategyProvider> getServiceType() {
    return ResilienceStrategyProvider.class;
  }

}
