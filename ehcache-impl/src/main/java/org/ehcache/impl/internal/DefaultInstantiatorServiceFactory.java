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
package org.ehcache.impl.internal;

import org.ehcache.core.spi.service.InstantiatorService;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.internal.classes.DefaultInstantiatorService;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

@Component
public class DefaultInstantiatorServiceFactory implements ServiceFactory<InstantiatorService> {

  @Override
  public int rank() {
    return Integer.MIN_VALUE;
  }

  @Override
  public InstantiatorService create(ServiceCreationConfiguration<InstantiatorService, ?> configuration) {
    return new DefaultInstantiatorService();
  }

  @Override
  public Class<? extends InstantiatorService> getServiceType() {
    return DefaultInstantiatorService.class;
  }
}
