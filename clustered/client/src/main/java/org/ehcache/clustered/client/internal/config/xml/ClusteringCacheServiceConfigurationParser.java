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
import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.NAMESPACE;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.XML_SCHEMA;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.TC_CLUSTERED_NAMESPACE_PREFIX;

/**
 * Provides parsing support for the {@code <service>} elements representing a {@link ClusteredStore.Provider ClusteringService}.
 *
 * @see ClusteredCacheConstants#XSD
 */
@Component
public class ClusteringCacheServiceConfigurationParser extends BaseConfigParser<ClusteredStoreConfiguration> implements CacheServiceConfigurationParser<ClusteredStore.Provider> {

  public static final String CLUSTERED_STORE_ELEMENT_NAME = "clustered-store";
  public static final String CONSISTENCY_ATTRIBUTE_NAME = "consistency";

  public ClusteringCacheServiceConfigurationParser() {
    super(ClusteredStoreConfiguration.class);
  }

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ServiceConfiguration<ClusteredStore.Provider> parseServiceConfiguration(Element fragment, ClassLoader classLoader) {
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
    return unparseConfig(serviceConfiguration);
  }

  @Override
  protected Element createRootElement(Document doc, ClusteredStoreConfiguration clusteredStoreConfiguration) {
    Consistency consistency = clusteredStoreConfiguration.getConsistency();
    Element rootElement = doc.createElementNS(getNamespace().toString(), TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTERED_STORE_ELEMENT_NAME);
    rootElement.setAttribute(CONSISTENCY_ATTRIBUTE_NAME, consistency.name().toLowerCase());
    return rootElement;
  }

}
