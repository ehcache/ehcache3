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

import org.ehcache.config.Configuration;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.pany.ehcache.serializer.TestSerializer;
import com.pany.ehcache.serializer.TestSerializer2;
import com.pany.ehcache.serializer.TestSerializer3;
import com.pany.ehcache.serializer.TestSerializer4;

import java.io.IOException;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultSerializationProviderConfigurationParserTest extends ServiceProvideConfigurationParserTestBase {

  public DefaultSerializationProviderConfigurationParserTest() {
    super(new DefaultSerializationProviderConfigurationParser());
  }

  @Test
  public void parseServiceCreationConfiguration() throws SAXException, JAXBException, ParserConfigurationException, IOException, ClassNotFoundException {
    Configuration xmlConfig = parseXmlConfiguration("/configs/default-serializer.xml");

    assertThat(xmlConfig.getServiceCreationConfigurations()).hasSize(1);


    ServiceCreationConfiguration configuration = xmlConfig.getServiceCreationConfigurations().iterator().next();

    assertThat(configuration).isExactlyInstanceOf(DefaultSerializationProviderConfiguration.class);

    DefaultSerializationProviderConfiguration factoryConfiguration = (DefaultSerializationProviderConfiguration) configuration;
    Map<Class<?>, Class<? extends Serializer<?>>> defaultSerializers = factoryConfiguration.getDefaultSerializers();
    assertThat(defaultSerializers).hasSize(4);
    assertThat(defaultSerializers.get(CharSequence.class)).isEqualTo(TestSerializer.class);
    assertThat(defaultSerializers.get(Number.class)).isEqualTo(TestSerializer2.class);
    assertThat(defaultSerializers.get(Long.class)).isEqualTo(TestSerializer3.class);
    assertThat(defaultSerializers.get(Integer.class)).isEqualTo(TestSerializer4.class);
  }
}
