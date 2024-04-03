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
package org.ehcache.impl.internal.classes;

import org.ehcache.core.spi.service.InstantiatorService;
import org.ehcache.impl.internal.classes.commonslang.reflect.ConstructorUtils;
import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceProvider;

public class DefaultInstantiatorService implements InstantiatorService {

  @Override
  public void start(ServiceProvider<Service> serviceProvider) {

  }

  @Override
  public void stop() {

  }

  @Override
  public <T> T instantiate(Class<T> clazz, Object ... arguments) {
    try {
      return ConstructorUtils.invokeConstructor(clazz, arguments);
    } catch (ReflectiveOperationException e) {
      throw new RuntimeException(e);
    }
  }
}
