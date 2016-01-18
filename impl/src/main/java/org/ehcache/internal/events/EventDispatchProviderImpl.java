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

import org.ehcache.config.event.EventDispatcherFactoryConfiguration;
import org.ehcache.events.EventDispatchProvider;
import org.ehcache.events.OrderedEventDispatcher;
import org.ehcache.events.UnorderedEventDispatcher;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.ExecutionService;
import org.ehcache.spi.service.ServiceDependencies;

import java.util.concurrent.LinkedBlockingQueue;

import static org.ehcache.internal.executor.ExecutorUtil.shutdown;

@ServiceDependencies(ExecutionService.class)
public class EventDispatchProviderImpl implements EventDispatchProvider {

  private final String threadPoolAlias;
  ExecutionService executionService;
  private volatile OrderedEventDispatcher orderedDispatcher;
  private volatile UnorderedEventDispatcher unorderedDispatcher;

  public EventDispatchProviderImpl(EventDispatcherFactoryConfiguration configuration) {
    this.threadPoolAlias = configuration.getThreadPoolAlias();
  }

  public EventDispatchProviderImpl() {
    this.threadPoolAlias = null;
  }

  @Override
  public synchronized OrderedEventDispatcher getOrderedEventDispatcher() {
    if (orderedDispatcher == null) {
      try {
        orderedDispatcher = new OrderedEventDispatcher(executionService.getOrderedExecutor(threadPoolAlias, new LinkedBlockingQueue<Runnable>()));
      } catch (IllegalArgumentException e) {
        if (threadPoolAlias == null) {
          throw new IllegalStateException("No default executor could be found for Event Dispatcher", e);
        } else {
          throw new IllegalStateException("No executor named '" + threadPoolAlias + "' could be found for Event Dispatcher", e);
        }
      }
    }
    return orderedDispatcher;
  }

  @Override
  public synchronized UnorderedEventDispatcher getUnorderedEventDispatcher() {
    if (unorderedDispatcher == null) {
      try {
        unorderedDispatcher = new UnorderedEventDispatcher(executionService.getUnorderedExecutor(threadPoolAlias, new LinkedBlockingQueue<Runnable>()));
      } catch (IllegalArgumentException e) {
        if (threadPoolAlias == null) {
          throw new IllegalStateException("No default executor could be found for Event Dispatcher", e);
        } else {
          throw new IllegalStateException("No executor named '" + threadPoolAlias + "' could be found for Event Dispatcher", e);
        }
      }
    }
    return unorderedDispatcher;
  }

  @Override
  public void start(final ServiceProvider serviceProvider) {
    this.executionService = serviceProvider.getService(ExecutionService.class);
  }

  @Override
  public void stop() {
    if (orderedDispatcher != null) {
      shutdown(orderedDispatcher.getExecutorService());
    }
    if (unorderedDispatcher != null) {
      shutdown(unorderedDispatcher.getExecutorService());
    }
  }
}
