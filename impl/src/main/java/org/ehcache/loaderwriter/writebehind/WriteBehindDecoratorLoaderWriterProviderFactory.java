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
package org.ehcache.loaderwriter.writebehind;

import org.ehcache.config.writebehind.WriteBehindConfiguration;
import org.ehcache.config.writebehind.WriteBehindDecoratorLoaderWriterProvider;
import org.ehcache.spi.ServiceLocator;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author Abhilash
 *
 */
public class WriteBehindDecoratorLoaderWriterProviderFactory implements ServiceFactory<WriteBehindDecoratorLoaderWriterProvider> {
  
  private volatile WriteBehindDecoratorLoaderWriter<?, ?> loaderWriter = null;

  @Override
  public WriteBehindDecoratorLoaderWriterProvider create(ServiceConfiguration<WriteBehindDecoratorLoaderWriterProvider> serviceConfiguration, ServiceLocator serviceLocator) {
    return new WriteBehindDecoratorLoaderWriterProvider() {
      
      @Override
      public void stop() {
        // no op
        
      }
      
      @Override
      public void start(ServiceConfiguration<?> config, ServiceProvider serviceProvider) {
        // no op
        
      }
      
      @Override
      public <K, V> WriteBehindDecoratorLoaderWriter<K, V> createWriteBehindDecoratorLoaderWriter(CacheLoaderWriter<K, V> cacheLoaderWriter, WriteBehindConfiguration configuration) {
        loaderWriter = new WriteBehindDecoratorLoaderWriter<K, V>(cacheLoaderWriter, configuration);
        return (WriteBehindDecoratorLoaderWriter<K, V>)loaderWriter;
      }

      @Override
      public void releaseWriteBehindDecoratorCacheLoaderWriter(CacheLoaderWriter<?, ?> cacheLoaderWriter) {
        if(loaderWriter != null) loaderWriter.getWriteBehindQueue().stop();
      }
    };
  }

  @Override
  public Class<WriteBehindDecoratorLoaderWriterProvider> getServiceType() {
    return WriteBehindDecoratorLoaderWriterProvider.class;
  }

}
