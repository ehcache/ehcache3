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
import org.ehcache.impl.config.event.DefaultCacheEventDispatcherConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.model.CacheType;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class DefaultCacheEventDispatcherConfigurationParserTest {

  @Test
  public void parseServiceConfiguration() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = new XmlConfiguration(getClass().getResource("/configs/ehcache-cacheEventListener.xml")).getCacheConfigurations().get("template1");

    DefaultCacheEventDispatcherConfiguration eventDispatcherConfig =
      findSingletonAmongst(DefaultCacheEventDispatcherConfiguration.class, cacheConfiguration.getServiceConfigurations());

    assertThat(eventDispatcherConfig).isNotNull();
    assertThat(eventDispatcherConfig.getThreadPoolAlias()).isEqualTo("listeners-pool");
  }

  @Test
  public void unparseServiceConfiguration() {
    CacheConfiguration<?, ?> cacheConfig =
      newCacheConfigurationBuilder(Object.class, Object.class, heap(10)).add(new DefaultCacheEventDispatcherConfiguration("foo")).build();
    CacheType cacheType = new CacheType();
    cacheType = new DefaultCacheEventDispatcherConfigurationParser().unparseServiceConfiguration(cacheConfig, cacheType);

    assertThat(cacheType.getListeners().getDispatcherThreadPool()).isEqualTo("foo");
  }
}
