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

import org.ehcache.config.event.CacheEventListenerConfigurationBuilder;
import org.ehcache.event.CacheEvent;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.event.EventType;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import org.ehcache.config.event.DefaultCacheEventListenerConfiguration;

import static org.hamcrest.collection.IsArray.array;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

/**
 * @author rism
 */
public class CacheEventListenerConfigurationBuilderTest {
  @Test
  public void builderTest() {
    Set<EventType> eventTypeSet = new HashSet<EventType>();
    eventTypeSet.add(EventType.CREATED);
    eventTypeSet.add(EventType.UPDATED);
    CacheEventListenerConfigurationBuilder cacheEventListenerConfigurationBuilder = CacheEventListenerConfigurationBuilder
            .newEventListenerConfiguration(ListenerObject.class, eventTypeSet)
            .constructedWith("Yay! Listeners!")
            .firingMode(EventFiring.ASYNCHRONOUS)
            .eventOrdering(EventOrdering.UNORDERED);

    DefaultCacheEventListenerConfiguration config = cacheEventListenerConfigurationBuilder.build();

    assertThat(config.fireOn(), is(eventTypeSet));
    assertThat(config.firingMode(), is(EventFiring.ASYNCHRONOUS));
    assertThat(config.orderingMode(), is(EventOrdering.UNORDERED));
    assertThat(config.getClazz(), equalTo((Class) ListenerObject.class));
    assertThat(config.getArguments(), array(is((Object) "Yay! Listeners!")));
  }

  public static class ListenerObject implements CacheEventListener<Object, Object> {

    private final String message;

    public ListenerObject(String message) {
      this.message = message;
    }

    @Override
    public void onEvent(CacheEvent<Object, Object> event) {
      //noop
    }

    @Override
    public String toString() {
      return message;
    }
  }
}