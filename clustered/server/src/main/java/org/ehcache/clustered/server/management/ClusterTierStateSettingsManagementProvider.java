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
import org.terracotta.management.service.monitoring.registry.provider.ClientBindingManagementProvider;

import java.util.Collection;
import java.util.Collections;

@Named("ClusterTierClientStateSettings")
@RequiredContext({@Named("consumerId"), @Named("clientId"), @Named("type")})
class ClusterTierStateSettingsManagementProvider extends ClientBindingManagementProvider<ClusterTierClientStateBinding> {

  ClusterTierStateSettingsManagementProvider() {
    super(ClusterTierClientStateBinding.class);
  }

  @Override
  protected ExposedClusterTierStateBinding internalWrap(Context context, ClusterTierClientStateBinding managedObject) {
    return new ExposedClusterTierStateBinding(context, managedObject);
  }

  private static class ExposedClusterTierStateBinding extends ExposedClientBinding<ClusterTierClientStateBinding> {

    ExposedClusterTierStateBinding(Context context, ClusterTierClientStateBinding clientBinding) {
      super(context, clientBinding);
    }

    @Override
    public Context getContext() {
      return super.getContext().with("type", "ClusterTierClientState");
    }

    @Override
    public Collection<? extends Descriptor> getDescriptors() {
      ClusterTierClientState clientState = getClientBinding().getValue();
      return Collections.singleton(new Settings(getContext())
        .set("attached", clientState.isAttached())
        .set("store", clientState.getStoreIdentifier())
      );
    }
  }

}
