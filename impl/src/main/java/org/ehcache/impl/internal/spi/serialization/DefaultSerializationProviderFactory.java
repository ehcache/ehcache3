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

package org.ehcache.impl.internal.spi.serialization;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.spi.serialization.SerializationProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

/**
 * @author Ludovic Orban
 */
@Component
public class DefaultSerializationProviderFactory implements ServiceFactory<SerializationProvider> {

  @Override
  public DefaultSerializationProvider create(ServiceCreationConfiguration<SerializationProvider, ?> configuration) {
    if (configuration != null && !(configuration instanceof DefaultSerializationProviderConfiguration)) {
      throw new IllegalArgumentException("Expected a configuration of type DefaultSerializationProviderConfiguration but got " + configuration
          .getClass()
          .getSimpleName());
    }
    return new DefaultSerializationProvider((DefaultSerializationProviderConfiguration) configuration);
  }

  @Override
  public Class<SerializationProvider> getServiceType() {
    return SerializationProvider.class;
  }
}
