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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SimpleServiceConfiguration implements ServiceCreationConfiguration<SimpleServiceProvider, Collection<Service>> {

  private final Collection<Service> services = new ArrayList<>();

  public SimpleServiceConfiguration(Class<? extends Service> clazz, List<String> unparsedArgs) throws Exception {
    this.services.add(ReflectionHelper.instantiate(clazz, unparsedArgs));
  }

  private SimpleServiceConfiguration(Collection<Service> services) {
    this.services.addAll(services);
  }

  public Collection<Service> getServices() {
    return Collections.unmodifiableCollection(services);
  }

  @Override
  public Class<SimpleServiceProvider> getServiceType() {
    return SimpleServiceProvider.class;
  }

  @Override
  public Collection<Service> derive() {
    return services;
  }

  @Override
  public ServiceCreationConfiguration<SimpleServiceProvider, ?> build(Collection<Service> representation) {
    return new SimpleServiceConfiguration(representation);
  }

  @Override
  public boolean compatibleWith(ServiceCreationConfiguration<?, ?> other) {
    // TODO WTF, seriously?
    if (other instanceof SimpleServiceConfiguration) {
      Collection<Service> copy = new ArrayList<>(services);
      this.services.addAll(((SimpleServiceConfiguration) other).services);
      ((SimpleServiceConfiguration) other).services.addAll(copy);
    }
    return false;
  }
}
