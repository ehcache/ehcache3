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

import org.ehcache.clustered.server.ClientState;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;
import org.terracotta.management.model.capabilities.descriptors.Settings;
import org.terracotta.management.model.context.Context;
import org.terracotta.management.registry.Named;
import org.terracotta.management.registry.RequiredContext;
import org.terracotta.management.service.monitoring.registry.provider.ClientBindingManagementProvider;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.TreeSet;

@Named("ClientStateSettings")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
class ClientStateSettingsManagementProvider extends ClientBindingManagementProvider<ClientStateBinding> {

  ClientStateSettingsManagementProvider() {
    super(ClientStateBinding.class);
  }

  @Override
  protected ExposedClientStateBinding internalWrap(Context context, ClientStateBinding managedObject) {
    return new ExposedClientStateBinding(context, managedObject);
  }

  private static class ExposedClientStateBinding extends ExposedClientBinding<ClientStateBinding> {

    ExposedClientStateBinding(Context context, ClientStateBinding clientBinding) {
      super(context.with("type", "ClientState"), clientBinding);
    }

    @Override
    public Collection<? extends Descriptor> getDescriptors() {
      ClientState clientState = getClientBinding().getValue();
      return Collections.singleton(new Settings(getContext())
        .set("attached", clientState.isAttached())
      );
    }
  }

}
