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

package org.ehcache.spi.event;

import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.event.DefaultCacheEventListenerBuilder;
import org.ehcache.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.junit.Test;

import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class DefaultCacheEventListenerFactoryTest {

  @Test
  public void testCacheConfigUsage() {
    Set<EventType> eventTypeSet = new HashSet<EventType>();
    eventTypeSet.add(EventType.CREATED);
    eventTypeSet.add(EventType.UPDATED);

    DefaultCacheEventListenerBuilder listenerBuilder = DefaultCacheEventListenerBuilder.newCacheEventListenerBuilder();
    listenerBuilder.withEventsToFireOn(EnumSet.copyOf(eventTypeSet));
    listenerBuilder.withEventOrdering(EventOrdering.UNORDERED);
    listenerBuilder.withEventFiringMode(EventFiring.ASYNCHRONOUS);
    DefaultCacheEventListenerConfiguration cacheEventListenerConfiguration = listenerBuilder.build(ListenerObject.class);
    final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("foo",
            CacheConfigurationBuilder.newCacheConfigurationBuilder()
                .addServiceConfig(cacheEventListenerConfiguration)
                .buildConfig(Object.class, Object.class)).build(true);
    final Collection<?> bar = manager.getCache("foo", Object.class, Object.class).getRuntimeConfiguration().getServiceConfigurations();
    assertThat(bar.iterator().next().getClass().toString(), is(ListenerObject.object.toString()));
  }

  public static class ListenerObject implements CacheEventListener<Object, Object> {
    private static final Object object = new Object() {
      @Override
      public String toString() {
        return "class "+ org.ehcache.config.event.DefaultCacheEventListenerConfiguration.class.getName();
      }
    };

    @Override
    public void onEvent(CacheEvent<Object, Object> event) {
      //noop
    }
  }
}
