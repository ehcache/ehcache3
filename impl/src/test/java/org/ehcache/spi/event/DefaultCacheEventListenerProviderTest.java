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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.event.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.CacheEventListenerConfiguration;
import org.ehcache.event.EventType;
import org.ehcache.exceptions.StateTransitionException;
import org.ehcache.spi.service.ServiceUseConfiguration;
import org.hamcrest.core.Is;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class DefaultCacheEventListenerProviderTest {

  @Test
  public void testCacheConfigUsage() {
    Set<EventType> eventTypeSet = new HashSet<EventType>();
    eventTypeSet.add(EventType.CREATED);
    eventTypeSet.add(EventType.UPDATED);

    CacheEventListenerConfigurationBuilder listenerBuilder = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, eventTypeSet).unordered().asynchronous();
    final CacheManager manager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("foo",
            CacheConfigurationBuilder.newCacheConfigurationBuilder()
                .add(listenerBuilder)
                .buildConfig(Object.class, Object.class)).build(true);
    final Collection<?> bar = manager.getCache("foo", Object.class, Object.class).getRuntimeConfiguration().getServiceConfigurations();
    assertThat(bar.iterator().next().getClass().toString(), is(ListenerObject.object.toString()));
  }

  @Test
  public void testAddingCacheEventListenerConfigurationAtManagerLevelThrows() {
    CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheEventListenerConfiguration cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.CREATED).unordered().asynchronous().build();
    cacheManagerBuilder.using(cacheEventListenerConfiguration);
    CacheManager cacheManager = null;
    try {
      cacheManager = cacheManagerBuilder.build(true);
    } catch (StateTransitionException ste) {
      // expected
      assertThat(ste.getMessage(), Is.is("DefaultCacheEventListenerConfiguration must not be provided at CacheManager level"));
    } finally {
      if(cacheManager != null) {
        cacheManager.close();
      }
    }
  }

  @Test
  public void testAddingCacheEventListenerConfigurationAtCacheLevel() {
    CacheManagerBuilder<CacheManager> cacheManagerBuilder = CacheManagerBuilder.newCacheManagerBuilder();
    CacheEventListenerConfiguration cacheEventListenerConfiguration = CacheEventListenerConfigurationBuilder
        .newEventListenerConfiguration(ListenerObject.class, EventType.CREATED).unordered().asynchronous().build();
    CacheManager cacheManager = cacheManagerBuilder.build(true);
    final Cache<Long, String> cache = cacheManager.createCache("cache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder()
            .add(cacheEventListenerConfiguration)
            .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                .heap(100, EntryUnit.ENTRIES).build())
            .buildConfig(Long.class, String.class));
    Collection<ServiceUseConfiguration<?>> serviceConfiguration = cache.getRuntimeConfiguration()
        .getServiceConfigurations();
    assertThat(serviceConfiguration, IsCollectionContaining.<ServiceUseConfiguration<?>>hasItem(instanceOf(DefaultCacheEventListenerConfiguration.class)));
    cacheManager.close();
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