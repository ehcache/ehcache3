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
package org.ehcache.impl.internal.loaderwriter.writebehind;

import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.config.loaderwriter.writebehind.WriteBehindProviderConfiguration;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindProvider;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceDependencies;
import org.ehcache.spi.service.ServiceProvider;
import org.osgi.service.component.annotations.Component;

/**
 * @author Abhilash
 *
 */
@Component
public class WriteBehindProviderFactory implements ServiceFactory<WriteBehindProvider> {

  @Override
  public WriteBehindProvider create(ServiceCreationConfiguration<WriteBehindProvider> configuration) {
    if (configuration == null) {
      return new Provider();
    } else if (configuration instanceof WriteBehindProviderConfiguration) {
      return new Provider(((WriteBehindProviderConfiguration) configuration).getThreadPoolAlias());
    } else {
      throw new IllegalArgumentException("WriteBehind configuration must not be provided at CacheManager level");
    }
  }

  @ServiceDependencies(ExecutionService.class)
  public static class Provider implements WriteBehindProvider {

    private final String threadPoolAlias;
    private volatile ExecutionService executionService;

    protected Provider() {
      this(null);
    }

    protected Provider(String threadPoolAlias) {
      this.threadPoolAlias = threadPoolAlias;
    }

    @Override
    public void stop() {
      // no-op

    }

    @Override
    public void start(ServiceProvider<Service> serviceProvider) {
      executionService = serviceProvider.getService(ExecutionService.class);
    }

    @Override
    public <K, V> WriteBehind<K, V> createWriteBehindLoaderWriter(CacheLoaderWriter<K, V> cacheLoaderWriter, WriteBehindConfiguration configuration) {
      if (cacheLoaderWriter == null) {
        throw new NullPointerException("WriteBehind requires a non null CacheLoaderWriter.");
      }
      return new StripedWriteBehind<>(executionService, threadPoolAlias, configuration, cacheLoaderWriter);
    }

    @Override
    public void releaseWriteBehindLoaderWriter(CacheLoaderWriter<?, ?> cacheLoaderWriter) {
      if(cacheLoaderWriter != null) {
        ((WriteBehind)cacheLoaderWriter).stop();
      }
    }
  }

  @Override
  public Class<WriteBehindProvider> getServiceType() {
    return WriteBehindProvider.class;
  }

}
