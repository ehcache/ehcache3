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

package org.ehcache.xml.service;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.xml.CoreServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.EventType;
import org.ehcache.xml.model.ListenersConfig;
import org.ehcache.xml.model.ListenersType;

import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.ehcache.xml.XmlConfiguration.getClassForName;

public class DefaultCacheEventListenerConfigurationParser implements CoreServiceConfigurationParser {

  @Override
  public <K, V> CacheConfigurationBuilder<K, V> parseServiceConfiguration(CacheTemplate cacheDefinition, ClassLoader cacheClassLoader,
                                                                          CacheConfigurationBuilder<K, V> cacheBuilder) throws ClassNotFoundException {
    ListenersConfig listenersConfig = cacheDefinition.listenersConfig();
    if(listenersConfig != null && listenersConfig.listeners() != null) {
      for (ListenersType.Listener listener : listenersConfig.listeners()) {
        Set<org.ehcache.event.EventType> eventSetToFireOn = listener.getEventsToFireOn().stream()
          .map(EventType::value).map(org.ehcache.event.EventType::valueOf).collect(toSet());
        @SuppressWarnings("unchecked")
        Class<CacheEventListener<?, ?>> cacheEventListenerClass = (Class<CacheEventListener<?, ?>>) getClassForName(listener.getClazz(), cacheClassLoader);
        CacheEventListenerConfigurationBuilder listenerBuilder = CacheEventListenerConfigurationBuilder
          .newEventListenerConfiguration(cacheEventListenerClass, eventSetToFireOn)
          .firingMode(EventFiring.valueOf(listener.getEventFiringMode().value()))
          .eventOrdering(EventOrdering.valueOf(listener.getEventOrderingMode().value()));
        cacheBuilder = cacheBuilder.add(listenerBuilder);
      }
    }

    return cacheBuilder;
  }
}

