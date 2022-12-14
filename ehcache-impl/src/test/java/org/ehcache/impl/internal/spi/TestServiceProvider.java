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

package org.ehcache.impl.internal.spi;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.ehcache.spi.service.ServiceProvider;
import org.ehcache.spi.service.Service;

/**
 *
 */
public final class TestServiceProvider {

  public static ServiceProvider<Service> providerContaining(final Service... services) {
    final Map<Class<? extends Service>, Service> servicesMap = new HashMap<>();

    for (Service s : services) {
      servicesMap.put(s.getClass(), s);
    }

    return new ServiceProvider<Service>() {

      @Override
      public <T extends Service> T getService(Class<T> serviceType) {
        return serviceType.cast(servicesMap.get(serviceType));
      }

      @Override
      public <U extends Service> Collection<U> getServicesOfType(final Class<U> serviceType) {
        throw new UnsupportedOperationException(".getServicesOfType not implemented");
      }
    };
  }
}
