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

package org.ehcache.clustered.client.internal.store;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

/**
 * Factory to create instances of {@link ClusteredStore.Provider}.
 */
@Component
public class ClusteredStoreProviderFactory implements ServiceFactory<ClusteredStore.Provider> {
  @Override
  public ClusteredStore.Provider create(final ServiceCreationConfiguration<ClusteredStore.Provider, ?> configuration) {
    return new ClusteredStore.Provider();
  }

  @Override
  public Class<ClusteredStore.Provider> getServiceType() {
    return ClusteredStore.Provider.class;
  }
}
