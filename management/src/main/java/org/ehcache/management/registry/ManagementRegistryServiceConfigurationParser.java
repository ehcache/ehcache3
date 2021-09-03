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
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;
import java.net.URL;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

public class ManagementRegistryServiceConfigurationParser extends BaseConfigParser<DefaultManagementRegistryConfiguration> implements CacheManagerServiceConfigurationParser<ManagementRegistryService> {

  private static final String NAMESPACE = "http://www.ehcache.org/v3/management";
  private static final URI NAMESPACE_URI = URI.create(NAMESPACE);
  private static final URL XML_SCHEMA = ManagementRegistryServiceConfigurationParser.class.getResource("/ehcache-management-ext.xsd");
  private static final String MANAGEMENT_NAMESPACE_PREFIX = "mgm:";
  private static final String MANAGEMENT_ELEMENT_NAME = "management";
  private static final String CACHE_MANAGER_ATTRIBUTE_NAME = "cache-manager-alias";
  private static final String COLLECTOR_EXECUTOR_ATTRIBUTE_NAME = "collector-executor-alias";
  private static final String TAGS_NAME = "tags";
  private static final String TAG_NAME = "tag";

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE_URI;
  }

  @Override
  public ServiceCreationConfiguration<ManagementRegistryService, ?> parseServiceCreationConfiguration(Element fragment, ClassLoader classLoader) {
    if ("management".equals(fragment.getLocalName())) {
      DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration();

      // ATTR: cache-manager-alias
      if (fragment.hasAttribute("cache-manager-alias")) {
        registryConfiguration.setCacheManagerAlias(attr(fragment, "cache-manager-alias"));
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

      return registryConfiguration;

    } else {
      throw new XmlConfigurationException(String.format(
          "XML configuration element <%s> in <%s> is not supported",
          fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  private static String attr(Element element, String name) {
    String s = element.getAttribute(name);
    return s == null || s.equals("") ? null : s;
  }

  private static String val(Element element) {
    return element.hasChildNodes() ? element.getFirstChild().getNodeValue() : null;
  }

  @Override
  public Class<ManagementRegistryService> getServiceType() {
    return ManagementRegistryService.class;
  }

  @Override
  public Element unparseServiceCreationConfiguration(ServiceCreationConfiguration<ManagementRegistryService, ?> serviceCreationConfiguration) {
    return unparseConfig(serviceCreationConfiguration);
  }

  @Override
  protected Element createRootElement(Document doc, DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration) {
    Element rootElement = doc.createElementNS(NAMESPACE,MANAGEMENT_NAMESPACE_PREFIX + MANAGEMENT_ELEMENT_NAME);
    rootElement.setAttribute(CACHE_MANAGER_ATTRIBUTE_NAME, defaultManagementRegistryConfiguration.getCacheManagerAlias());
    rootElement.setAttribute(COLLECTOR_EXECUTOR_ATTRIBUTE_NAME, defaultManagementRegistryConfiguration.getCollectorExecutorAlias());
    processManagementTags(doc, rootElement, defaultManagementRegistryConfiguration);
    return rootElement;
  }

  private void processManagementTags(Document doc, Element parent, DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration) {
    if (!defaultManagementRegistryConfiguration.getTags().isEmpty()) {
      Element tagsName = doc.createElement(MANAGEMENT_NAMESPACE_PREFIX + TAGS_NAME);
      for (String tag : defaultManagementRegistryConfiguration.getTags()) {
        Element tagName = doc.createElement(MANAGEMENT_NAMESPACE_PREFIX + TAG_NAME);
        tagName.setTextContent(tag);
        tagsName.appendChild(tagName);
      }
      parent.appendChild(tagsName);
    }
  }

}
