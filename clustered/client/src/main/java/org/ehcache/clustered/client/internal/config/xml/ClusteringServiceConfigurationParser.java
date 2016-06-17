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
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.ClusteringServiceConfiguration.PoolDefinition;
import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.CacheServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.*;

/**
 * Provides parsing support for the {@code <service>} elements representing a {@link ClusteringService ClusteringService}.
 *
 * @see ClusteredCacheConstants#XSD
 */
public class ClusteringServiceConfigurationParser implements CacheManagerServiceConfigurationParser<ClusteringService>,
                                                              CacheServiceConfigurationParser<ClusteredStore.Provider> {

  public static final String CLUSTERED_STORE_ELEMENT_NAME = "clustered-store";
  public static final String CONSISTENCY_ATTRIBUTE_NAME = "consistency";

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

      Map<String, PoolDefinition> sharedPools = null;
      String defaultServerResource = null;
      URI connectionUri = null;
      boolean autoCreate = false;
      final NodeList childNodes = fragment.getChildNodes();
      for (int i = 0; i < childNodes.getLength(); i++) {
        final Node item = childNodes.item(i);
        if (Node.ELEMENT_NODE == item.getNodeType()) {
          if ("connection".equals(item.getLocalName())) {
            /*
             * <connection> is a required element in the XSD
             */
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

          } else if ("server-side-config".equals(item.getLocalName())) {
            /*
             * <server-side-config> is an optional element
             */
            ServerSideConfig config = processServerSideConfig(item);
            autoCreate = config.autoCreate;
            defaultServerResource = config.defaultServerResource;
            sharedPools = config.pools;
          }
        }
      }
      try {
        return new ClusteringServiceConfiguration(connectionUri, autoCreate, defaultServerResource, sharedPools);
      } catch (IllegalArgumentException e) {
        throw new XmlConfigurationException(e);
      }
    }
    throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
  }

  private ServerSideConfig processServerSideConfig(Node serverSideConfigElement) {
    ServerSideConfig serverSideConfig = new ServerSideConfig();
    serverSideConfig.autoCreate = Boolean.parseBoolean(((Element) serverSideConfigElement).getAttribute("auto-create"));
    final NodeList serverSideNodes = serverSideConfigElement.getChildNodes();
    for (int i = 0; i < serverSideNodes.getLength(); i++) {
      final Node item = serverSideNodes.item(i);
      if (Node.ELEMENT_NODE == item.getNodeType()) {
        String nodeLocalName = item.getLocalName();
        if ("default-resource".equals(nodeLocalName)) {
          serverSideConfig.defaultServerResource = ((Element)item).getAttribute("from");

        } else if ("shared-pool".equals(nodeLocalName)) {
          Element sharedPoolElement = (Element)item;
          String poolName = sharedPoolElement.getAttribute("name");     // required
          Attr fromAttr = sharedPoolElement.getAttributeNode("from");   // optional
          String fromResource = (fromAttr == null ? null : fromAttr.getValue());
          Attr unitAttr = sharedPoolElement.getAttributeNode("unit");   // optional - default 'B'
          String unit = (unitAttr == null ? "B" : unitAttr.getValue());
          MemoryUnit memoryUnit = MemoryUnit.valueOf(unit.toUpperCase(Locale.ENGLISH));

          String quantityValue = sharedPoolElement.getFirstChild().getNodeValue();
          long quantity;
          try {
            quantity = Long.parseLong(quantityValue);
          } catch (NumberFormatException e) {
            throw new XmlConfigurationException("Magnitude of value specified for <shared-pool name=\""
                + poolName + "\"> is too large");
          }

          if (serverSideConfig.pools == null) {
            serverSideConfig.pools = new HashMap<String, PoolDefinition>();
          }

          PoolDefinition poolDefinition;
          if (fromResource == null) {
            poolDefinition = new PoolDefinition(quantity, memoryUnit);
          } else {
            poolDefinition = new PoolDefinition(quantity, memoryUnit, fromResource);
          }

          if (serverSideConfig.pools.put(poolName, poolDefinition) != null) {
            throw new XmlConfigurationException("Duplicate definition for <shared-pool name=\"" + poolName + "\">");
          }
        }
      }
    }
    return serverSideConfig;
  }

  private static final class ServerSideConfig {
    private boolean autoCreate;
    private String defaultServerResource;
    private Map<String, PoolDefinition> pools;
  }
}
