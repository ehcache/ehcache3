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
package org.ehcache.internal.events;

import org.ehcache.events.EventDispatchProvider;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.spi.service.ServiceFactory;

/**
 * @author rism
 */
public class EventDispatchProviderFactory implements ServiceFactory<EventDispatchProvider> {
  @Override
  public EventDispatchProvider create(ServiceCreationConfiguration<EventDispatchProvider> configuration) {
    return new EventDispatchProviderImpl();
  }

  @Override
  public Class<EventDispatchProvider> getServiceType() {
    return EventDispatchProvider.class;
  }
}
