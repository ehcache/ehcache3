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
package org.ehcache.management.providers.actions;

import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.registry.action.AbstractActionManagementProvider;
import org.terracotta.management.registry.action.ExposedObject;

@Named("ActionsCapability")
@RequiredContext({@Named("cacheManagerName"), @Named("cacheName")})
public class EhcacheActionProvider extends AbstractActionManagementProvider<CacheBinding> {

  private final ManagementRegistryServiceConfiguration registryServiceConfiguration;

  public EhcacheActionProvider(ManagementRegistryServiceConfiguration registryServiceConfiguration) {
    super(CacheBinding.class);
    this.registryServiceConfiguration = registryServiceConfiguration;
  }

  @Override
  protected ExposedObject<CacheBinding> wrap(CacheBinding managedObject) {
    return new EhcacheActionWrapper(registryServiceConfiguration, managedObject);
  }

}
