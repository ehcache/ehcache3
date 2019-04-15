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

package org.ehcache.impl.internal.store.tiering;

import org.ehcache.core.spi.service.ServiceFactory;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.osgi.service.component.annotations.Component;

/**
 * @author Ludovic Orban
 */
@Component
public class TieredStoreProviderFactory implements ServiceFactory<TieredStore.Provider> {

  @Override
  public TieredStore.Provider create(ServiceCreationConfiguration<TieredStore.Provider> configuration) {
    return new TieredStore.Provider();
  }

  @Override
  public Class<TieredStore.Provider> getServiceType() {
    return TieredStore.Provider.class;
  }
}
