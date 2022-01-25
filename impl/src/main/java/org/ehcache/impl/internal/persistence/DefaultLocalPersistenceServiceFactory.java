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

package org.ehcache.impl.internal.persistence;

import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

/**
 * @author Alex Snaps
 */
@Component
@ServiceFactory.RequiresConfiguration
public class DefaultLocalPersistenceServiceFactory implements ServiceFactory<LocalPersistenceService> {

  @Override
  public DefaultLocalPersistenceService create(final ServiceCreationConfiguration<LocalPersistenceService, ?> serviceConfiguration) {
    return new DefaultLocalPersistenceService((DefaultPersistenceConfiguration) serviceConfiguration);
  }

  @Override
  public Class<LocalPersistenceService> getServiceType() {
    return LocalPersistenceService.class;
  }
}
