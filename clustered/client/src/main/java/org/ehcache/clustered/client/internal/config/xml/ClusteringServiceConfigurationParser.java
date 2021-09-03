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
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.store.ClusteredStore;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.clustered.common.ServerSideConfiguration.Pool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.CacheServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.TimeType;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.*;
import static org.ehcache.xml.XmlModel.convertToJavaTimeUnit;

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
   * <p>
   * This method presumes the element presented is valid according to the XSD.
   *
   * @param fragment the XML fragment to process
   *
   * @return a {@link org.ehcache.clustered.client.config.ClusteringServiceConfiguration ClusteringServiceConfiguration}
   */
  @Override
  public ServiceCreationConfiguration<ClusteringService> parseServiceCreationConfiguration(final Element fragment) {

    if ("cluster".equals(fragment.getLocalName())) {

      ServerSideConfig serverConfig = null;
      URI connectionUri = null;
      Duration getTimeout = null, putTimeout = null, connectionTimeout = null;
      final NodeList childNodes = fragment.getChildNodes();
      for (int i = 0; i < childNodes.getLength(); i++) {
        final Node item = childNodes.item(i);
        if (Node.ELEMENT_NODE == item.getNodeType()) {
          switch (item.getLocalName()) {
            case "connection":
              /*
               * <connection> is a required element in the XSD
               */
              final Attr urlAttribute = ((Element) item).getAttributeNode("url");
              final String urlValue = urlAttribute.getValue();
              try {
                connectionUri = new URI(urlValue);
              } catch (URISyntaxException e) {
                throw new XmlConfigurationException(
                  String.format("Value of %s attribute on XML configuration element <%s> in <%s> is not a valid URI - '%s'",
                    urlAttribute.getName(), item.getNodeName(), fragment.getTagName(), connectionUri), e);
              }

              break;
            case "read-timeout":
              /*
               * <read-timeout> is an optional element
               */
              getTimeout = processTimeout(fragment, item);

              break;
            case "write-timeout":
              /*
               * <write-timeout> is an optional element
               */
              putTimeout = processTimeout(fragment, item);

              break;
            case "connection-timeout":
              /*
               * <connection-timeout> is an optional element
               */
              connectionTimeout = processTimeout(fragment, item);

              break;
            case "server-side-config":
              /*
               * <server-side-config> is an optional element
               */
              serverConfig = processServerSideConfig(item);
              break;
            default:
              throw new XmlConfigurationException(
                String.format("Unknown XML configuration element <%s> in <%s>",
                              item.getNodeName(), fragment.getTagName()));
          }
        }
      }

      try {
        Timeouts timeouts = getTimeouts(getTimeout, putTimeout, connectionTimeout);
        if (serverConfig == null) {
          return new ClusteringServiceConfiguration(connectionUri, timeouts);
        }

        ServerSideConfiguration serverSideConfiguration;
        if (serverConfig.defaultServerResource == null) {
          serverSideConfiguration = new ServerSideConfiguration(serverConfig.pools);
        } else {
          serverSideConfiguration = new ServerSideConfiguration(serverConfig.defaultServerResource, serverConfig.pools);
        }

        return new ClusteringServiceConfiguration(connectionUri, timeouts, serverConfig.autoCreate, serverSideConfiguration);
      } catch (IllegalArgumentException e) {
        throw new XmlConfigurationException(e);
      }
    }
    throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
  }

  private Timeouts getTimeouts(Duration getTimeout, Duration putTimeout, Duration connectionTimeout) {
    TimeoutsBuilder builder = TimeoutsBuilder.timeouts();
    if (getTimeout != null) {
      builder.read(getTimeout);
    }
    if(putTimeout != null) {
      builder.write(putTimeout);
    }
    if(connectionTimeout != null) {
      builder.connection(connectionTimeout);
    }
    return builder.build();
  }

  private Duration processTimeout(Element parentElement, Node timeoutNode) {
    try {
      // <xxx-timeout> are direct subtype of ehcache:time-type; use JAXB to interpret it
      JAXBContext context = JAXBContext.newInstance(TimeType.class.getPackage().getName());
      Unmarshaller unmarshaller = context.createUnmarshaller();
      JAXBElement<TimeType> jaxbElement = unmarshaller.unmarshal(timeoutNode, TimeType.class);

      TimeType timeType = jaxbElement.getValue();
      BigInteger amount = timeType.getValue();
      if (amount.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
        throw new XmlConfigurationException(
            String.format("Value of XML configuration element <%s> in <%s> exceeds allowed value - %s",
                timeoutNode.getNodeName(), parentElement.getTagName(), amount));
      }
      return Duration.of(amount.longValue(), convertToJavaTimeUnit(timeType.getUnit()));

    } catch (JAXBException e) {
      throw new XmlConfigurationException(e);
    }
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

          Pool poolDefinition;
          if (fromResource == null) {
            poolDefinition = new Pool(memoryUnit.toBytes(quantity));
          } else {
            poolDefinition = new Pool(memoryUnit.toBytes(quantity), fromResource);
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
    private boolean autoCreate = false;
    private String defaultServerResource = null;
    private final Map<String, Pool> pools = new HashMap<>();
  }
}
