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
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.model.ConfigType;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

public class CacheManagerPersistenceConfigurationParserTest {

  @Test
  public void parseServiceCreationConfiguration() throws SAXException, JAXBException, ParserConfigurationException, IOException, ClassNotFoundException {
    Configuration xmlConfig = new XmlConfiguration(getClass().getResource("/configs/disk-persistent-cache.xml"));

    List<CacheManagerPersistenceConfiguration> serviceConfig = xmlConfig.getServiceCreationConfigurations().stream()
      .filter(i -> CacheManagerPersistenceConfiguration.class.equals(i.getClass()))
      .map(CacheManagerPersistenceConfiguration.class::cast).collect(toList());
    assertThat(serviceConfig).hasSize(1);

    CacheManagerPersistenceConfiguration providerConfiguration = serviceConfig.iterator().next();
    assertThat(providerConfiguration.getRootDirectory()).isEqualTo(new File("some/dir"));
  }


  @Test
  public void unparseServiceCreationConfiguration() {
    Configuration config = ConfigurationBuilder.newConfigurationBuilder()
      .withService(new CacheManagerPersistenceConfiguration(new File("foo"))).build();
    ConfigType configType = new CacheManagerPersistenceConfigurationParser().unparseServiceCreationConfiguration(config, new ConfigType());

    assertThat(configType.getPersistence().getDirectory()).isEqualTo("foo");
  }
}
