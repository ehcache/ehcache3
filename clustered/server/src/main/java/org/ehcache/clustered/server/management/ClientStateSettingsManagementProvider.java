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

@Named("ClientStateSettings")
@RequiredContext({@Named("consumerId"), @Named("type"), @Named("alias")})
class ClientStateSettingsManagementProvider extends ClientBindingManagementProvider<ClientDescriptorBinding> {

  ClientStateSettingsManagementProvider() {
    super(ClientDescriptorBinding.class);
  }

  @Override
  protected ExposedClientStateBinding internalWrap(Context context, ClientDescriptorBinding managedObject) {
    return new ExposedClientStateBinding(context, managedObject);
  }

  private static class ExposedClientStateBinding extends ExposedClientBinding<ClientDescriptorBinding> {

    ExposedClientStateBinding(Context context, ClientDescriptorBinding clientBinding) {
      super(context.with("type", "ClientState"), clientBinding);
    }

    @Override
    public Collection<? extends Descriptor> getDescriptors() {
      return Collections.singleton(new Settings(getContext()));
    }
  }

}
