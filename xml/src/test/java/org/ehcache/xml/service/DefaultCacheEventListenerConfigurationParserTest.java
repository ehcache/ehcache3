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
import org.ehcache.event.EventType;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.pany.ehcache.integration.TestCacheEventListener;

import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.ehcache.event.EventFiring.SYNCHRONOUS;
import static org.ehcache.event.EventOrdering.UNORDERED;

public class DefaultCacheEventListenerConfigurationParserTest extends ServiceConfigurationParserTestBase {

  public DefaultCacheEventListenerConfigurationParserTest() {
    super(new DefaultCacheEventListenerConfigurationParser());
  }

  @Test
  public void parseServiceConfiguration() throws SAXException, JAXBException, ParserConfigurationException, IOException, ClassNotFoundException {
    CacheConfiguration<?, ?> cacheConfiguration = getCacheDefinitionFrom("/configs/ehcache-cacheEventListener.xml", "bar");

    DefaultCacheEventListenerConfiguration listenerConfig =
      findSingletonAmongst(DefaultCacheEventListenerConfiguration.class, cacheConfiguration.getServiceConfigurations());

    assertThat(listenerConfig).isNotNull();
    assertThat(listenerConfig.getClazz()).isEqualTo(TestCacheEventListener.class);
    assertThat(listenerConfig.firingMode()).isEqualTo(SYNCHRONOUS);
    assertThat(listenerConfig.orderingMode()).isEqualTo(UNORDERED);
    assertThat(listenerConfig.fireOn()).containsExactlyInAnyOrder(EventType.values());
  }
}
