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

package org.ehcache.clustered.client.config.xml;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.config.xml.ClusteredCacheConstants.*;

/**
 * Provides parsing support for the {@code <service>} elements representing a {@link ClusteringService ClusteringService}.
 *
 * @author Clifford W. Johnson
 *
 * @see ClusteredCacheConstants#XSD
 */
public class ClusteringServiceConfigurationParser implements CacheManagerServiceConfigurationParser<ClusteringService> {

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  /**
   * Complete interpretation of the top-level elements defined in <code>{@value ClusteredCacheConstants#XSD}</code>.
   * This method is called only for those elements from the namespace set by {@link ClusteredCacheConstants#NAMESPACE}.
   *
   * @param fragment the XML fragment to process
   *
   * @return a {@link org.ehcache.clustered.client.config.ClusteringServiceConfiguration ClusteringServiceConfiguration}
   */
  @Override
  public ServiceCreationConfiguration<ClusteringService> parseServiceCreationConfiguration(final Element fragment) {
    /*
     * The presumptions in this code are:
     *   1) the Element is schema-validated
     *   2) 'connection' is defined as required (minOccurs=1, maxOccurs=1)
     *   3) 'connection.url' is defined as "required"
     *   4) no other elements or attributes are defined
     */
    if ("cluster".equals(fragment.getLocalName())) {

      URI connectionUri = null;
      final NodeList childNodes = fragment.getChildNodes();
      for (int i = 0; i < childNodes.getLength(); i++) {
        final Node item = childNodes.item(i);
        if (Node.ELEMENT_NODE == item.getNodeType()) {
          if ("connection".equals(item.getLocalName())) {
            final Attr urlAttribute = ((Element)item).getAttributeNode("url");
            final String urlValue = urlAttribute.getValue();
            try {
              connectionUri = new URI(urlValue);
            } catch (URISyntaxException e) {
              throw new XmlConfigurationException(
                  String.format("Value of %s attribute on XML configuration element <%s> in <%s> is not a valid URI - '%s'",
                      urlAttribute.getName(), fragment.getTagName(),
                      (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName()),
                      connectionUri), e);
            }
          }
        }
      }
      return new ClusteringServiceConfiguration(connectionUri, Collections.<String, PoolDefinition>emptyMap());
    }
    throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
  }
}
