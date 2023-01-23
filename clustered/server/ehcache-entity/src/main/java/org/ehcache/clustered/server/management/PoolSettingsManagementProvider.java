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
package org.ehcache.clustered.server.management;

import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.registry.ExposedObject;
import org.terracotta.management.service.monitoring.registry.provider.AliasBindingManagementProvider;

import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

@Named("PoolSettings")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
class PoolSettingsManagementProvider extends AliasBindingManagementProvider<PoolBinding> {

  PoolSettingsManagementProvider() {
    super(PoolBinding.class);
  }

  @Override
  public Collection<ExposedObject<PoolBinding>> getExposedObjects() {
    return super.getExposedObjects().stream().filter(e -> e.getTarget() != PoolBinding.ALL_SHARED).collect(Collectors.toList());
  }

  @Override
  protected ExposedPoolBinding internalWrap(Context context, PoolBinding managedObject) {
    return new ExposedPoolBinding(context, managedObject);
  }

  private static class ExposedPoolBinding extends ExposedAliasBinding<PoolBinding> {

    ExposedPoolBinding(Context context, PoolBinding binding) {
      super(context.with("type", "Pool"), binding);
    }

    @Override
    public Collection<? extends Descriptor> getDescriptors() {
      return getBinding() == PoolBinding.ALL_SHARED ?
        Collections.emptyList() :
        Collections.singleton(new Settings(getContext())
          .set("serverResource", getBinding().getValue().getServerResource())
          .set("size", getBinding().getValue().getSize())
          .set("allocationType", getBinding().getAllocationType().name().toLowerCase()));
    }
  }

}
