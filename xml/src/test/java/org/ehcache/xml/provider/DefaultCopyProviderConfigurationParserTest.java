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
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.CopierType;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.pany.ehcache.copier.Description;
import com.pany.ehcache.copier.DescriptionCopier;
import com.pany.ehcache.copier.Person;
import com.pany.ehcache.copier.PersonCopier;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.assertj.core.api.Assertions.assertThat;

public class DefaultCopyProviderConfigurationParserTest {

  @Test
  public void parseServiceCreationConfiguration() throws SAXException, JAXBException, ParserConfigurationException, IOException, ClassNotFoundException {
    Configuration xmlConfig = new XmlConfiguration(getClass().getResource("/configs/cache-copiers.xml"));

    assertThat(xmlConfig.getServiceCreationConfigurations()).hasSize(1);

    ServiceCreationConfiguration<?> configuration = xmlConfig.getServiceCreationConfigurations().iterator().next();

    assertThat(configuration).isExactlyInstanceOf(DefaultCopyProviderConfiguration.class);

    DefaultCopyProviderConfiguration factoryConfiguration = (DefaultCopyProviderConfiguration) configuration;
    Map<Class<?>, DefaultCopierConfiguration<?>> defaults = factoryConfiguration.getDefaults();
    assertThat(defaults).hasSize(2);
    assertThat(defaults.get(Description.class).getClazz()).isEqualTo(DescriptionCopier.class);
    assertThat(defaults.get(Person.class).getClazz()).isEqualTo((PersonCopier.class));
  }

  @Test
  public void unparseServiceCreationConfiguration() {
    DefaultCopyProviderConfiguration providerConfig = new DefaultCopyProviderConfiguration();
    providerConfig.addCopierFor(Description.class, DescriptionCopier.class);
    providerConfig.addCopierFor(Person.class, PersonCopier.class);

    Configuration config = ConfigurationBuilder.newConfigurationBuilder().addService(providerConfig).build();
    ConfigType configType = new DefaultCopyProviderConfigurationParser().unparseServiceCreationConfiguration(config, new ConfigType());

    List<CopierType.Copier> copiers = configType.getDefaultCopiers().getCopier();
    assertThat(copiers).hasSize(2);
    copiers.forEach(copier -> {
      if (copier.getType().equals(Description.class.getName())) {
        assertThat(copier.getValue()).isEqualTo(DescriptionCopier.class.getName());
      } else if (copier.getType().equals(Person.class.getName())) {
        assertThat(copier.getValue()).isEqualTo(PersonCopier.class.getName());
      } else {
        throw new AssertionError("Not expected");
      }
    });
  }
}
