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
import org.ehcache.xml.model.CacheType;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;

public class DefaultCacheEventDispatcherConfigurationParserTest extends ServiceConfigurationParserTestBase {

  public DefaultCacheEventDispatcherConfigurationParserTest() {
    super(new DefaultCacheEventDispatcherConfigurationParser());
  }

  @Test
  public void parseServiceConfiguration() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration =
      getCacheDefinitionFrom("/configs/ehcache-cacheEventListener.xml", "template1");

    DefaultCacheEventDispatcherConfiguration eventDispatcherConfig =
      findSingletonAmongst(DefaultCacheEventDispatcherConfiguration.class, cacheConfiguration.getServiceConfigurations());

    assertThat(eventDispatcherConfig).isNotNull();
    assertThat(eventDispatcherConfig.getThreadPoolAlias()).isEqualTo("listeners-pool");
  }

  @Test
  public void unparseServiceConfiguration() {
    CacheConfiguration<?, ?> cacheConfig =
      buildCacheConfigWithServiceConfig(new DefaultCacheEventDispatcherConfiguration("foo"));
    CacheType cacheType = new CacheType();
    cacheType = parser.unparseServiceConfiguration(cacheConfig, cacheType);

    assertThat(cacheType.getListeners().getDispatcherThreadPool()).isEqualTo("foo");
  }
}
