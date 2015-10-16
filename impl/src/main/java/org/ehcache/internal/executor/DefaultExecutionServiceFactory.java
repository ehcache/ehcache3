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

import org.ehcache.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.spi.service.ExecutionService;
import org.ehcache.spi.service.ExecutionServiceFactory;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 *
 * @author cdennis
 */
public class DefaultExecutionServiceFactory implements ExecutionServiceFactory {

  @Override
  public ExecutionService create(ServiceCreationConfiguration<ExecutionService> configuration) {
    if (configuration == null) {
      return new OnDemandExecutionService();
    } else if (configuration instanceof PooledExecutionServiceConfiguration) {
      throw new UnsupportedOperationException();
    } else {
      throw new IllegalArgumentException("Expected a configuration of type DefaultSerializationProviderConfiguration but got " + configuration
          .getClass()
          .getSimpleName());
    }
  }

  @Override
  public Class<ExecutionService> getServiceType() {
    return ExecutionService.class;
  }
  
}
