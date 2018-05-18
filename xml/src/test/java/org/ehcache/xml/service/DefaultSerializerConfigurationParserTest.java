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
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.xml.model.CacheEntryType;
import org.ehcache.xml.model.CacheType;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.pany.ehcache.copier.Description;
import com.pany.ehcache.copier.Person;
import com.pany.ehcache.serializer.TestSerializer3;
import com.pany.ehcache.serializer.TestSerializer4;

import java.io.IOException;
import java.util.Collection;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.spi.service.ServiceUtils.findAmongst;

public class DefaultSerializerConfigurationParserTest extends ServiceConfigurationParserTestBase {

  public DefaultSerializerConfigurationParserTest() {
    super(new DefaultSerializerConfigurationParser());
  }

  @Test
  public void parseServiceConfiguration() throws ClassNotFoundException, SAXException, ParserConfigurationException, IOException, JAXBException {
    CacheConfiguration<?, ?> cacheConfiguration = getCacheDefinitionFrom("/configs/default-serializer.xml", "foo");
    Collection<DefaultSerializerConfiguration> copierConfigs =
      findAmongst(DefaultSerializerConfiguration.class, cacheConfiguration.getServiceConfigurations());
    assertThat(copierConfigs).hasSize(2);

    for(DefaultSerializerConfiguration copierConfig: copierConfigs) {
      if(copierConfig.getType() == DefaultSerializerConfiguration.Type.KEY) {
        assertThat(copierConfig.getClazz()).isEqualTo(TestSerializer3.class);
      } else {
        assertThat(copierConfig.getClazz()).isEqualTo(TestSerializer4.class);
      }
    }
  }

  @Test
  public void unparseServiceConfiguration() {
    CacheConfiguration<?, ?> cacheConfig = newCacheConfigurationBuilder(Description.class, Person.class, heap(10))
      .add(new DefaultSerializerConfiguration(TestSerializer3.class, DefaultSerializerConfiguration.Type.KEY))
      .add(new DefaultSerializerConfiguration(TestSerializer4.class, DefaultSerializerConfiguration.Type.VALUE))
      .build();

    CacheType cacheType = new CacheType();
    CacheEntryType keyType = new CacheEntryType();
    keyType.setValue("foo");
    cacheType.setKeyType(keyType);
    CacheEntryType valueType = new CacheEntryType();
    valueType.setValue("bar");
    cacheType.setValueType(valueType);

    cacheType = parser.unparseServiceConfiguration(cacheConfig, cacheType);

    assertThat(cacheType.getKeyType().getSerializer()).isEqualTo(TestSerializer3.class.getName());
    assertThat(cacheType.getValueType().getSerializer()).isEqualTo(TestSerializer4.class.getName());
  }
}
