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

package org.ehcache.impl.internal.spi.event;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.core.events.CacheEventListenerConfiguration;
import org.ehcache.event.EventType;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class DefaultCacheEventListenerProviderTest {

  @Test
  public void testCacheConfigUsage() {
    Set<EventType> eventTypeSet = new HashSet<>();
    eventTypeSet.add(EventType.CREATED);
    eventTypeSet.add(EventType.UPDATED);

    CacheEventListenerConfigurationBuilder listenerBuilder = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, eventTypeSet).unordered().asynchronous();
    final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("foo",
            CacheConfigurationBuilder.newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
                .withService(listenerBuilder)
                .build()).build(true);
    final Collection<?> bar = manager.getCache("foo", Object.class, Object.class).getRuntimeConfiguration().getServiceConfigurations();
    assertThat(bar.iterator().next().getClass().toString(), is(ListenerObject.object.toString()));
  }

  @Test
  public void testAddingCacheEventListenerConfigurationAtCacheLevel() {
    CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheEventListenerConfiguration<?> cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.CREATED).unordered().asynchronous().build();
    CacheManager cacheManager = cacheManagerBuilder.build(true);
    final Cache<Long, String> cache = cacheManager.createCache("cache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(100))
            .withService(cacheEventListenerConfiguration)
            .build());
    Collection<ServiceConfiguration<?, ?>> serviceConfiguration = cache.getRuntimeConfiguration()
        .getServiceConfigurations();
    assertThat(serviceConfiguration, IsCollectionContaining.<ServiceConfiguration<?, ?>>hasItem(instanceOf(DefaultCacheEventListenerConfiguration.class)));
    cacheManager.close();
  }

  public static class ListenerObject implements CacheEventListener<Object, Object> {
    private static final Object object = new Object() {
      @Override
      public String toString() {
        return "class "+ DefaultCacheEventListenerConfiguration.class.getName();
      }
    };

    @Override
    public void onEvent(CacheEvent<? extends Object, ? extends Object> event) {
      //noop
    }
  }
}
