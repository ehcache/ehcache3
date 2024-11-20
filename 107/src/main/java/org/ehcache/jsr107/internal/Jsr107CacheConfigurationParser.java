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

package org.ehcache.jsr107.internal;


import org.ehcache.jsr107.config.ConfigurationElementState;
import org.ehcache.jsr107.config.Jsr107CacheConfiguration;
import org.ehcache.jsr107.Jsr107Service;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.xml.CacheServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

/**
 * Jsr107CacheConfigurationParser
 */
@Component
public class Jsr107CacheConfigurationParser implements CacheServiceConfigurationParser<Jsr107Service> {

  private static final URI NAMESPACE = URI.create("http://www.ehcache.org/v3/jsr107");
  private static final URL XML_SCHEMA = Jsr107CacheConfigurationParser.class.getResource("/ehcache-107-ext.xsd");
  private static final String MANAGEMENT_ENABLED_ATTRIBUTE = "enable-management";
  private static final String STATISTICS_ENABLED_ATTRIBUTE = "enable-statistics";

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceConfiguration<Jsr107Service, ?> parseServiceConfiguration(Element fragment, ClassLoader classLoader) {
    String localName = fragment.getLocalName();
    if ("mbeans".equals(localName)) {
      ConfigurationElementState managementEnabled = ConfigurationElementState.UNSPECIFIED;
      ConfigurationElementState statisticsEnabled = ConfigurationElementState.UNSPECIFIED;
      if (fragment.hasAttribute(MANAGEMENT_ENABLED_ATTRIBUTE)) {
        managementEnabled = Boolean.parseBoolean(fragment.getAttribute(MANAGEMENT_ENABLED_ATTRIBUTE)) ? ConfigurationElementState.ENABLED : ConfigurationElementState.DISABLED;
      }
      if (fragment.hasAttribute(STATISTICS_ENABLED_ATTRIBUTE)) {
        statisticsEnabled = Boolean.parseBoolean(fragment.getAttribute(STATISTICS_ENABLED_ATTRIBUTE)) ? ConfigurationElementState.ENABLED : ConfigurationElementState.DISABLED;
      }
      return new Jsr107CacheConfiguration(statisticsEnabled, managementEnabled);
    } else {
      throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
          fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  @Override
  public Class<Jsr107Service> getServiceType() {
    return Jsr107Service.class;
  }

  @Override
  public Element unparseServiceConfiguration(ServiceConfiguration<Jsr107Service, ?> serviceConfiguration) {
    throw new XmlConfigurationException("XML translation of JSR-107 cache elements are not supported");
  }

}
