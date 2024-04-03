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
package org.ehcache.core.spi;

import org.ehcache.spi.service.Service;

import java.util.function.UnaryOperator;

public interface ServiceLocatorUtils {

  static <T extends Service> void withServiceLocator(T service, ServiceTask<T> task) throws Exception {
    withServiceLocator(service, UnaryOperator.identity(), task);
  }

  static <T extends Service> void withServiceLocator(T service, UnaryOperator<ServiceLocator.DependencySet> dependencies, ServiceTask<T> task) throws Exception {
    ServiceLocator serviceLocator = dependencies.apply(ServiceLocator.dependencySet().with(service)).build();
    serviceLocator.startAllServices();
    try {
      task.execute(service);
    } finally {
      serviceLocator.stopAllServices();
    }
  }

  @FunctionalInterface
  interface ServiceTask<T extends Service> {

    void execute(T service) throws Exception;
  }

}
