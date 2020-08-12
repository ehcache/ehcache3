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
import org.ehcache.xml.ConfigurationParser;
import org.ehcache.xml.CoreServiceCreationConfigurationParser;
import org.ehcache.xml.model.ConfigType;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URL;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import static org.ehcache.config.builders.ConfigurationBuilder.newConfigurationBuilder;

public class ServiceProvideConfigurationParserTestBase {

  ClassLoader classLoader = this.getClass().getClassLoader();
  ConfigurationBuilder managerBuilder = newConfigurationBuilder();
  CoreServiceCreationConfigurationParser parser;

  public ServiceProvideConfigurationParserTestBase(CoreServiceCreationConfigurationParser parser) {
    this.parser = parser;
  }

  protected Configuration parseXmlConfiguration(String resourcePath) throws SAXException, JAXBException, ParserConfigurationException, IOException, ClassNotFoundException {
    URL resource = this.getClass().getResource(resourcePath);
    ConfigType configType = new ConfigurationParser().parseXml(resource.toExternalForm());

    managerBuilder = parser.parseServiceCreationConfiguration(configType, classLoader, managerBuilder);
    return managerBuilder.build();
  }

}
