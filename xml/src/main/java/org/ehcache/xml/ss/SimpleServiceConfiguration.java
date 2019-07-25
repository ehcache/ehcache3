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
package org.ehcache.xml.ss;

import org.ehcache.spi.service.Service;
import org.ehcache.spi.service.ServiceCreationConfiguration;

import java.util.List;

public class SimpleServiceConfiguration implements ServiceCreationConfiguration<SimpleServiceProvider, Service> {

  private final Service service;

  public SimpleServiceConfiguration(Class<? extends Service> clazz, List<String> unparsedArgs) throws Exception {
    this.service = ReflectionHelper.instantiate(clazz, unparsedArgs);
  }

  private SimpleServiceConfiguration(Service service) {
    this.service = service;
  }

  public Service getService() {
    return service;
  }

  @Override
  public Class<SimpleServiceProvider> getServiceType() {
    return SimpleServiceProvider.class;
  }

  @Override
  public Service derive() {
    return service;
  }

  @Override
  public ServiceCreationConfiguration<SimpleServiceProvider, ?> build(Service representation) {
    return new SimpleServiceConfiguration(representation);
  }

  @Override
  public boolean compatibleWith(ServiceCreationConfiguration<?, ?> other) {
    return true; // supports many instances
  }
}
