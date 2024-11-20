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
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.impl.config.loaderwriter.writebehind.WriteBehindProviderConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.model.ConfigType;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;

public class WriteBehindProviderConfigurationParserTest {

  @Test
  public void parseServiceCreationConfiguration() throws SAXException, JAXBException, ParserConfigurationException, IOException, ClassNotFoundException {
    Configuration xmlConfig = new XmlConfiguration(getClass().getResource("/configs/writebehind-cache.xml"));

    assertThat(xmlConfig.getServiceCreationConfigurations()).hasSize(1);

    ServiceCreationConfiguration<?, ?> configuration = xmlConfig.getServiceCreationConfigurations().iterator().next();

    assertThat(configuration).isExactlyInstanceOf(WriteBehindProviderConfiguration.class);

    WriteBehindProviderConfiguration providerConfiguration = (WriteBehindProviderConfiguration) configuration;
    assertThat(providerConfiguration.getThreadPoolAlias()).isEqualTo("write-behind-pool");
  }

  @Test
  public void unparseServiceCreationConfiguration() {
    ConfigType configType = new ConfigType();
    Configuration config = ConfigurationBuilder.newConfigurationBuilder()
      .withService(new WriteBehindProviderConfiguration("foo")).build();
    configType = new WriteBehindProviderConfigurationParser().unparseServiceCreationConfiguration(config, configType);

    assertThat(configType.getWriteBehind().getThreadPool()).isEqualTo("foo");
  }
}
