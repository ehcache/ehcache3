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
import org.ehcache.xml.spi.CacheServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Jsr107CacheConfigurationParser
 */
@Component
public class Jsr107CacheConfigurationParser extends Jsr107Parser<Jsr107CacheConfiguration> implements CacheServiceConfigurationParser<Jsr107Service, Jsr107CacheConfiguration> {

  private static final String MANAGEMENT_ENABLED_ATTRIBUTE = "enable-management";
  private static final String STATISTICS_ENABLED_ATTRIBUTE = "enable-statistics";

  @Override
  public Jsr107CacheConfiguration parse(Element fragment, ClassLoader classLoader) {
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
  public Element safeUnparse(Document target, Jsr107CacheConfiguration serviceConfiguration) {
    throw new XmlConfigurationException("XML translation of JSR-107 cache elements are not supported");
  }

}
