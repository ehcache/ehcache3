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
package org.ehcache.config.event;

import org.ehcache.events.CacheEventDispatcherFactory;
import org.ehcache.spi.service.ServiceCreationConfiguration;

/**
 *
 * @author cdennis
 */
public class CacheEventDispatcherFactoryConfiguration implements ServiceCreationConfiguration<CacheEventDispatcherFactory> {

  private final String orderedExecutorAlias;
  private final String unorderedExecutorAlias;

  public CacheEventDispatcherFactoryConfiguration(String orderedExecutorAlias, String unorderedExecutorAlias) {
    this.orderedExecutorAlias = orderedExecutorAlias;
    this.unorderedExecutorAlias = unorderedExecutorAlias;
  }
  
  public String getOrderedExecutorAlias() {
    return orderedExecutorAlias;
  }
  
  public String getUnorderedExecutorAlias() {
    return unorderedExecutorAlias;
  }
  
  @Override
  public Class<CacheEventDispatcherFactory> getServiceType() {
    return CacheEventDispatcherFactory.class;
  }
}
