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
package org.ehcache.management.registry;

import org.ehcache.management.ManagementRegistryService;
import org.ehcache.management.config.DefaultStatisticsProviderConfiguration;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.XmlModel;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.terracotta.management.registry.ManagementProvider;
import org.w3c.dom.Element;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class ManagementRegistryServiceConfigurationParser implements CacheManagerServiceConfigurationParser<ManagementRegistryService> {

  private static final String NAMESPACE = "http://www.ehcache.org/v3/management";
  private static final URI NAMESPACE_URI = URI.create(NAMESPACE);
  private static final URL XML_SCHEMA = ManagementRegistryServiceConfigurationParser.class.getResource("/ehcache-management-ext.xsd");

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE_URI;
  }

  @Override
  public ServiceCreationConfiguration<ManagementRegistryService> parseServiceCreationConfiguration(Element fragment) {
    if ("management".equals(fragment.getLocalName())) {
      DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration();

      // ATTR: cache-manager-alias
      if (fragment.hasAttribute("cache-manager-alias")) {
        registryConfiguration.setCacheManagerAlias(attr(fragment, "cache-manager-alias"));
      }

      // ATTR: statistics-executor-alias
      if (fragment.hasAttribute("statistics-executor-alias")) {
        registryConfiguration.setStatisticsExecutorAlias(attr(fragment, "statistics-executor-alias"));
      }

      // ATTR: collector-executor-alias
      if (fragment.hasAttribute("collector-executor-alias")) {
        registryConfiguration.setCollectorExecutorAlias(attr(fragment, "collector-executor-alias"));
      }

      // tags
      for (Element tags : NodeListIterable.elements(fragment, NAMESPACE, "tags")) {
        // tag
        for (Element tag : NodeListIterable.elements(tags, NAMESPACE, "tag")) {
          String val = val(tag);
          if (val != null && !val.isEmpty()) {
            registryConfiguration.addTag(val);
          }
        }
      }

      // statistics-configurations
      for (Element statisticConfigurations : NodeListIterable.elements(fragment, NAMESPACE, "statistics-configurations")) {

        // statistics-configuration
        for (Element statisticConfiguration : NodeListIterable.elements(statisticConfigurations, NAMESPACE, "statistics-configuration")) {

          // ATTR: provider
          Class<?> providerType;
          try {
            providerType = getClass().getClassLoader().loadClass(attr(statisticConfiguration, "provider", EhcacheStatisticsProviderConfiguration.class.getName()));
          } catch (ClassNotFoundException e) {
            throw new IllegalStateException("Unable to load class " + statisticConfiguration.getAttribute("provider") + " : " + e.getMessage(), e);
          }
          if (!ManagementProvider.class.isAssignableFrom(providerType)) {
            throw new IllegalStateException("Class " + statisticConfiguration.getAttribute("provider") + " is not a " + ManagementProvider.class.getSimpleName());
          }

          DefaultStatisticsProviderConfiguration providerConfiguration = new DefaultStatisticsProviderConfiguration(providerType.asSubclass(ManagementProvider.class));

          // average-window
          for (Element averageWindow : NodeListIterable.elements(statisticConfiguration, NAMESPACE, "average-window")) {
            providerConfiguration.setAverageWindowDuration(
              Long.parseLong(val(averageWindow, String.valueOf(providerConfiguration.averageWindowDuration()))),
              unit(averageWindow, providerConfiguration.averageWindowUnit()));
          }

          // history-interval
          for (Element historyInterval : NodeListIterable.elements(statisticConfiguration, NAMESPACE, "history-interval")) {
            providerConfiguration.setHistoryInterval(
              Long.parseLong(val(historyInterval, String.valueOf(providerConfiguration.historyInterval()))),
              unit(historyInterval, providerConfiguration.historyIntervalUnit()));
          }

          // history-size
          for (Element historySize : NodeListIterable.elements(statisticConfiguration, NAMESPACE, "history-size")) {
            providerConfiguration.setHistorySize(Integer.parseInt(val(historySize, String.valueOf(providerConfiguration.historySize()))));
          }

          // time-to-disable
          for (Element timeToDisable : NodeListIterable.elements(statisticConfiguration, NAMESPACE, "time-to-disable")) {
            providerConfiguration.setTimeToDisable(
              Long.parseLong(val(timeToDisable, String.valueOf(providerConfiguration.timeToDisable()))),
              unit(timeToDisable, providerConfiguration.timeToDisableUnit()));
          }

          registryConfiguration.addConfiguration(providerConfiguration);
        }
      }

      return registryConfiguration;

    } else {
      throw new XmlConfigurationException(String.format(
          "XML configuration element <%s> in <%s> is not supported",
          fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  private static String attr(Element element, String name, String def) {
    String s = element.getAttribute(name);
    return s == null || s.equals("") ? def : s;
  }

  private static String attr(Element element, String name) {
    return attr(element, name, null);
  }

  private static String val(Element element) {
    return element.hasChildNodes() ? element.getFirstChild().getNodeValue() : null;
  }

  private static String val(Element element, String def) {
    return element.hasChildNodes() ? element.getFirstChild().getNodeValue() : def;
  }

  private static TimeUnit unit(Element element, TimeUnit def) {
    String s = attr(element, "unit");
    return s == null ? def : XmlModel.convertToJavaTimeUnit(org.ehcache.xml.model.TimeUnit.fromValue(s));
  }

}
