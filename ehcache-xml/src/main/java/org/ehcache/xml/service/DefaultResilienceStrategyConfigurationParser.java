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

import org.ehcache.impl.config.resilience.DefaultResilienceStrategyConfiguration;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheType;

import static org.ehcache.xml.XmlConfiguration.getClassForName;

public class DefaultResilienceStrategyConfigurationParser
  extends SimpleCoreServiceConfigurationParser<String, String, DefaultResilienceStrategyConfiguration> {

  @SuppressWarnings({"unchecked", "rawtypes"})
  public DefaultResilienceStrategyConfigurationParser() {
    super(DefaultResilienceStrategyConfiguration.class,
      CacheTemplate::resilienceStrategy,
      (config, loader) -> new DefaultResilienceStrategyConfiguration((Class<? extends ResilienceStrategy>) getClassForName(config, loader)),
      CacheType::getResilience, CacheType::setResilience, config -> {
        if(config.getInstance() == null) {
          return config.getClazz().getName();
        } else {
          throw new XmlConfigurationException("XML translation for instance based initialization for DefaultResilienceStrategyConfiguration is not supported");
        }
      });
  }
}
