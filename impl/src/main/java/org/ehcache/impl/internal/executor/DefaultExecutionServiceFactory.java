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
package org.ehcache.impl.internal.executor;

import org.ehcache.core.spi.service.ExecutionService;
import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

/**
 *
 * @author cdennis
 */
@Component
public class DefaultExecutionServiceFactory implements ServiceFactory<ExecutionService> {

  @Override
  public ExecutionService create(ServiceCreationConfiguration<ExecutionService, ?> configuration) {
    if (configuration == null) {
      return new OnDemandExecutionService();
    } else if (configuration instanceof PooledExecutionServiceConfiguration) {
      return new PooledExecutionService((PooledExecutionServiceConfiguration) configuration);
    } else {
      throw new IllegalArgumentException("Expected a configuration of type PooledExecutionServiceConfiguration but got " + configuration
          .getClass()
          .getSimpleName());
    }
  }

  @Override
  public Class<ExecutionService> getServiceType() {
    return ExecutionService.class;
  }

}
