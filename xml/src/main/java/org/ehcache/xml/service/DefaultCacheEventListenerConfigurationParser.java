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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.xml.CoreServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.EventFiringType;
import org.ehcache.xml.model.EventOrderingType;
import org.ehcache.xml.model.EventType;
import org.ehcache.xml.model.ListenersConfig;
import org.ehcache.xml.model.ListenersType;

import java.util.Collection;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;
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
        cacheBuilder = cacheBuilder.withService(listenerBuilder);
      }
    }

    return cacheBuilder;
  }

  @Override
  public CacheType unparseServiceConfiguration(CacheConfiguration<?, ?> cacheConfiguration, CacheType cacheType) {
    Collection<DefaultCacheEventListenerConfiguration> serviceConfigs =
      findAmongst(DefaultCacheEventListenerConfiguration.class, cacheConfiguration.getServiceConfigurations());

    if (!serviceConfigs.isEmpty()) {
      ListenersType listenersType = cacheType.getListeners();
      if (listenersType == null) {
        listenersType = new ListenersType();
        cacheType.setListeners(listenersType);
      }

      Set<ListenersType.Listener> listeners = serviceConfigs.stream().map(serviceConfig -> {
        ListenersType.Listener listener = new ListenersType.Listener();
        if(serviceConfig.getInstance() == null) {
          return listener.withClazz(serviceConfig.getClazz().getName())
            .withEventFiringMode(EventFiringType.fromValue(serviceConfig.firingMode().name()))
            .withEventOrderingMode(EventOrderingType.fromValue(serviceConfig.orderingMode().name()))
            .withEventsToFireOn(serviceConfig.fireOn()
              .stream()
              .map(eventType -> EventType.fromValue(eventType.name()))
              .collect(toSet()));
        } else {
          throw new XmlConfigurationException("XML translation for instance based initialization for DefaultCacheEventListenerConfiguration is not supported");

        }
      }).collect(toSet());

      cacheType.withListeners(listenersType.withListener(listeners));
    }

    return cacheType;
  }
}

