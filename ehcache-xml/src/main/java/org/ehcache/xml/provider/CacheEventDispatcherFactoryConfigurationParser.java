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

package org.ehcache.xml.provider;

import org.ehcache.impl.config.event.CacheEventDispatcherFactoryConfiguration;
import org.ehcache.xml.model.ConfigType;

public class CacheEventDispatcherFactoryConfigurationParser extends ThreadPoolServiceCreationConfigurationParser<CacheEventDispatcherFactoryConfiguration> {

  public CacheEventDispatcherFactoryConfigurationParser() {
    super(CacheEventDispatcherFactoryConfiguration.class, ConfigType::getEventDispatch, ConfigType::setEventDispatch,
      CacheEventDispatcherFactoryConfiguration::new, CacheEventDispatcherFactoryConfiguration::getThreadPoolAlias);
  }
}
