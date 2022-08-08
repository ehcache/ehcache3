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

package org.ehcache.impl.internal.sizeof;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

/**
 * @author Abhilash
 *
 */
@Deprecated
@Component
public class DefaultSizeOfEngineProviderFactory implements ServiceFactory<org.ehcache.core.spi.store.heap.SizeOfEngineProvider> {

  @Override
  public org.ehcache.core.spi.store.heap.SizeOfEngineProvider create(ServiceCreationConfiguration<org.ehcache.core.spi.store.heap.SizeOfEngineProvider, ?> configuration) {
    long maxTraversals = org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_OBJECT_GRAPH_SIZE;
    long maxSize = org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration.DEFAULT_MAX_OBJECT_SIZE;
    if(configuration != null) {
      org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration sizeOfEngineConfiguration = (org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration)configuration;
      maxTraversals = sizeOfEngineConfiguration.getMaxObjectGraphSize();
      maxSize = sizeOfEngineConfiguration.getUnit().toBytes(sizeOfEngineConfiguration.getMaxObjectSize());
    }
    return new DefaultSizeOfEngineProvider(maxTraversals, maxSize);
  }

  @Override
  public Class<? extends org.ehcache.core.spi.store.heap.SizeOfEngineProvider> getServiceType() {
    return DefaultSizeOfEngineProvider.class;
  }

}
