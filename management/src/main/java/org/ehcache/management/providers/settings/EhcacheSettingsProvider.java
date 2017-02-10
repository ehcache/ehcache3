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
package org.ehcache.management.providers.settings;

import org.ehcache.CacheManager;
import org.ehcache.core.HumanReadable;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.providers.CacheBinding;
import org.ehcache.management.providers.CacheBindingManagementProvider;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.registry.action.Named;
import org.terracotta.management.registry.action.RequiredContext;

import java.util.ArrayList;
import java.util.Collection;

@Named("SettingsCapability")
@RequiredContext({@Named("cacheManagerName")})
public class EhcacheSettingsProvider extends CacheBindingManagementProvider {

  private final CacheManager cacheManager;

  public EhcacheSettingsProvider(ManagementRegistryServiceConfiguration configuration, CacheManager cacheManager) {
    super(configuration);
    this.cacheManager = cacheManager;
  }

  @Override
  protected ExposedCacheSettings wrap(CacheBinding cacheBinding) {
    return new ExposedCacheSettings(registryConfiguration, cacheBinding);
  }

  @Override
  public Collection<? extends Descriptor> getDescriptors() {
    Collection<Descriptor> descriptors = new ArrayList<Descriptor>(super.getDescriptors());
    descriptors.add(cacheManagerSettings());
    return descriptors;
  }

  private Descriptor cacheManagerSettings() {
    return new Settings()
        .set("cacheManagerDescription", ((HumanReadable)cacheManager.getRuntimeConfiguration()).readableString())
        .set("status", cacheManager.getStatus())
        .set("managementContext", new Settings(registryConfiguration.getContext()))
        .set("tags", registryConfiguration.getTags().toArray(new String[registryConfiguration.getTags().size()]));
  }

}
