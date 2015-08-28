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
import org.ehcache.events.OrderedEventDispatcher;
import org.ehcache.events.UnorderedEventDispatcher;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ThreadPoolsService;

/**
 * @author rism
 */
public class EventDispatchProviderImpl implements EventDispatchProvider {
  ServiceProvider serviceProvider;
  public EventDispatchProviderImpl() {
  }

  @Override
  public OrderedEventDispatcher createOrderedEventDispatchers() {
    ThreadPoolsService threadPoolsService = serviceProvider.getService(ThreadPoolsService.class);
    return new OrderedEventDispatcher(threadPoolsService.getEventsOrderedDeliveryExecutor());
  }

  @Override
  public UnorderedEventDispatcher createUnorderedEventDispatchers() {
    ThreadPoolsService threadPoolsService = serviceProvider.getService(ThreadPoolsService.class);
    return new UnorderedEventDispatcher(threadPoolsService.getEventsUnorderedDeliveryExecutor());
  }

  @Override
  public void start(final ServiceProvider serviceProvider) {
    this.serviceProvider = serviceProvider;
  }

  @Override
  public void stop() {
  }
}
