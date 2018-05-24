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
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.DomUtil;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.Objects;

public class ManagementRegistryServiceConfigurationParser implements CacheManagerServiceConfigurationParser<ManagementRegistryService> {

  private static final String NAMESPACE = "http://www.ehcache.org/v3/management";
  private static final URI NAMESPACE_URI = URI.create(NAMESPACE);
  private static final URL XML_SCHEMA = ManagementRegistryServiceConfigurationParser.class.getResource("/ehcache-management-ext.xsd");
  private static final String MANAGEMENT_NAMESPACE_PREFIX = "mgm";
  private static final String MANAGEMENT_ELEMENT_NAME = "management";
  private static final String CACHE_MANAGER_ATTRIBUTE_NAME = "cache-manager-alias";
  private static final String COLLECTOR_EXECUTOR_ATTRIBUTE_NAME = "collector-executor-alias";
  private static final String TAGS_NAME = "tags";
  private static final String TAG_NAME = "tag";
  private static final String COLON = ":";

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
  public Element unparseServiceCreationConfiguration(ServiceCreationConfiguration<ManagementRegistryService> serviceCreationConfiguration) {
    try {
      validateParametersForTranslationToServiceConfig(serviceCreationConfiguration);
      Document doc = createDocumentRoot();
      DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration = (DefaultManagementRegistryConfiguration) serviceCreationConfiguration;
      Element rootElement = createManagementElement(doc, defaultManagementRegistryConfiguration);
      processManagementTags(doc, rootElement, defaultManagementRegistryConfiguration);
      return rootElement;
    } catch (SAXException | ParserConfigurationException | IOException e) {
      throw new XmlConfigurationException(e);
    }
  }

  private void validateParametersForTranslationToServiceConfig(ServiceCreationConfiguration<ManagementRegistryService> serviceCreationConfiguration) {
    Objects.requireNonNull(serviceCreationConfiguration, "ServiceCreationConfiguration must not be NULL");
    if (!(serviceCreationConfiguration instanceof DefaultManagementRegistryConfiguration)) {
      throw new IllegalArgumentException("Parameter serviceCreationConfiguration must be of type DefaultManagementRegistryConfiguration."
                                         + "Provided type of parameter is : " + serviceCreationConfiguration.getClass());
    }
  }

  private Element createManagementElement(Document doc, DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration) {
    Element rootElement = doc.createElement(MANAGEMENT_NAMESPACE_PREFIX + COLON + MANAGEMENT_ELEMENT_NAME);
    rootElement.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + MANAGEMENT_NAMESPACE_PREFIX, NAMESPACE);
    rootElement.setAttribute(CACHE_MANAGER_ATTRIBUTE_NAME, defaultManagementRegistryConfiguration.getCacheManagerAlias());
    rootElement.setAttribute(COLLECTOR_EXECUTOR_ATTRIBUTE_NAME, defaultManagementRegistryConfiguration.getCollectorExecutorAlias());
    return rootElement;
  }

  private void processManagementTags(Document doc, Element parent, DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration) {
    if (!defaultManagementRegistryConfiguration.getTags().isEmpty()) {
      Element tagsName = doc.createElement(MANAGEMENT_NAMESPACE_PREFIX + COLON + TAGS_NAME);
      for (String tag : defaultManagementRegistryConfiguration.getTags()) {
        Element tagName = doc.createElement(MANAGEMENT_NAMESPACE_PREFIX + COLON + TAG_NAME);
        tagName.setTextContent(tag);
        tagsName.appendChild(tagName);
      }
      parent.appendChild(tagsName);
    }
  }

  private Document createDocumentRoot() throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilder domBuilder = DomUtil.createAndGetDocumentBuilder();
    Document doc = domBuilder.newDocument();
    return doc;
  }

}
