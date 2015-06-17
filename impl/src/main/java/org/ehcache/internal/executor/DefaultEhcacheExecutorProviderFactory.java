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
package org.ehcache.internal.executor;

import org.ehcache.config.thread.DefaultEhcacheExecutorProviderConfig;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;

public class DefaultEhcacheExecutorProviderFactory implements ServiceFactory<RevisedEhcacheExecutorProvider> {

  @Override
  public RevisedEhcacheExecutorProvider create(ServiceConfiguration<RevisedEhcacheExecutorProvider> serviceConfiguration, ServiceLocator serviceLocator) {
    if (serviceConfiguration == null) {
      return new DefaultEhcacheExecutorProvider();
    } else {
      DefaultEhcacheExecutorProviderConfig config = (DefaultEhcacheExecutorProviderConfig) serviceConfiguration;
      return new DefaultEhcacheExecutorProvider(config.getSharedCachedThreadPoolConfig(), config.getSharedScheduledThreadPoolCoreSize());
    }
  }

  @Override
  public Class<RevisedEhcacheExecutorProvider> getServiceType() {
    return RevisedEhcacheExecutorProvider.class;
  }

 
}
