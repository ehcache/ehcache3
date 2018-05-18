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
package org.ehcache.clustered.client.internal.config.xml;

import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.xml.CacheServiceConfigurationParser;
import org.ehcache.xml.DomUtil;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URI;
import java.util.Objects;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.NAMESPACE;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.XML_SCHEMA;

/**
 * Provides parsing support for the {@code <service>} elements representing a {@link ClusteredStore.Provider ClusteringService}.
 *
 * @see ClusteredCacheConstants#XSD
 */
public class ClusteringCacheServiceConfigurationParser implements CacheServiceConfigurationParser<ClusteredStore.Provider> {

  public static final String CLUSTERED_STORE_ELEMENT_NAME = "clustered-store";
  public static final String CONSISTENCY_ATTRIBUTE_NAME = "consistency";
  public static final String TC_CLUSTERED_NAMESPACE_PREFIX = "tc";
  public static final String COLON = ":";

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceConfiguration<ClusteredStore.Provider> parseServiceConfiguration(Element fragment) {
    if (CLUSTERED_STORE_ELEMENT_NAME.equals(fragment.getLocalName())) {
      if (fragment.hasAttribute(CONSISTENCY_ATTRIBUTE_NAME)) {
        return new ClusteredStoreConfiguration(Consistency.valueOf(fragment.getAttribute("consistency").toUpperCase()));
      } else {
        return new ClusteredStoreConfiguration();
      }
    }
    throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
      fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
  }

  @Override
  public Class<ClusteredStore.Provider> getServiceType() {
    return ClusteredStore.Provider.class;
  }

  @Override
  public Element unparseServiceConfiguration(ServiceConfiguration<ClusteredStore.Provider> serviceConfiguration) {
    try {
      validateParametersForTranslationToServiceConfig(serviceConfiguration);
      ClusteredStoreConfiguration clusteredStoreConfiguration = (ClusteredStoreConfiguration)serviceConfiguration;
      Consistency consistency = clusteredStoreConfiguration.getConsistency();
      Document doc = createDocumentRoot();
      Element defaultResourceElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + COLON + CLUSTERED_STORE_ELEMENT_NAME);
      defaultResourceElement.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + TC_CLUSTERED_NAMESPACE_PREFIX, getNamespace()
        .toString());
      defaultResourceElement.setAttribute(CONSISTENCY_ATTRIBUTE_NAME, consistency.name().toLowerCase());
      return defaultResourceElement;
    } catch (SAXException | ParserConfigurationException | IOException e) {
      throw new XmlConfigurationException(e);
    }
  }

  private void validateParametersForTranslationToServiceConfig(ServiceConfiguration<ClusteredStore.Provider> clusterStoreConfiguration) {
    Objects.requireNonNull(clusterStoreConfiguration, "ServiceConfiguration must not be NULL");
    if (!(clusterStoreConfiguration instanceof ClusteredStoreConfiguration)) {
      throw new IllegalArgumentException("Parameter serviceCreationConfiguration must be of type ClusteredStoreConfiguration."
                                         + "Provided type of parameter is : " + clusterStoreConfiguration.getClass());
    }
  }

  private Document createDocumentRoot() throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilder domBuilder = DomUtil.createAndGetDocumentBuilder();
    Document doc = domBuilder.newDocument();
    return doc;
  }
}
