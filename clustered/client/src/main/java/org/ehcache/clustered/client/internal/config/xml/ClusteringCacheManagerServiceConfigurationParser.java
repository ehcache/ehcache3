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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.config.Timeouts;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.ConnectionSource;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.TimeType;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


import java.io.IOException;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.NAMESPACE;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.XML_SCHEMA;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.TC_CLUSTERED_NAMESPACE_PREFIX;
import static org.ehcache.xml.XmlModel.convertToJavaTimeUnit;

/**
 * Provides parsing support for the {@code <service>} elements representing a {@link ClusteringService ClusteringService}.
 *
 * @see ClusteredCacheConstants#XSD
 */
@Component
public class ClusteringCacheManagerServiceConfigurationParser extends BaseConfigParser<ClusteringServiceConfiguration> implements CacheManagerServiceConfigurationParser<ClusteringService> {

  public static final String CLUSTER_ELEMENT_NAME = "cluster";
  public static final String CONNECTION_ELEMENT_NAME = "connection";
  public static final String CLUSTER_CONNECTION_ELEMENT_NAME = "cluster-connection";
  public static final String CLUSTER_TIER_MANAGER_ATTRIBUTE_NAME = "cluster-tier-manager";
  public static final String SERVER_ELEMENT_NAME = "server";
  public static final String HOST_ATTRIBUTE_NAME = "host";
  public static final String PORT_ATTRIBUTE_NAME = "port";
  public static final String READ_TIMEOUT_ELEMENT_NAME = "read-timeout";
  public static final String WRITE_TIMEOUT_ELEMENT_NAME = "write-timeout";
  public static final String CONNECTION_TIMEOUT_ELEMENT_NAME = "connection-timeout";
  public static final String URL_ATTRIBUTE_NAME = "url";
  public static final String DEFAULT_RESOURCE_ELEMENT_NAME = "default-resource";
  public static final String SHARED_POOL_ELEMENT_NAME = "shared-pool";
  public static final String SERVER_SIDE_CONFIG = "server-side-config";
  public static final String AUTO_CREATE_ATTRIBUTE_NAME = "auto-create";
  public static final String UNIT_ATTRIBUTE_NAME = "unit";
  public static final String NAME_ATTRIBUTE_NAME = "name";
  public static final String FROM_ATTRIBUTE_NAME = "from";
  public static final String DEFAULT_UNIT_ATTRIBUTE_VALUE = "seconds";

  public ClusteringCacheManagerServiceConfigurationParser() {
    super(ClusteringServiceConfiguration.class);
  }

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
   * <p>
   * This method presumes the element presented is valid according to the XSD.
   *
   * @param fragment the XML fragment to process
   * @param classLoader
   * @return a {@link org.ehcache.clustered.client.config.ClusteringServiceConfiguration ClusteringServiceConfiguration}
   */
  @Override
  public ServiceCreationConfiguration<ClusteringService> parseServiceCreationConfiguration(final Element fragment, ClassLoader classLoader) {

    if ("cluster".equals(fragment.getLocalName())) {

      ClusteringCacheManagerServiceConfigurationParser.ServerSideConfig serverConfig = null;
      URI connectionUri = null;
      List<InetSocketAddress> serverAddresses = new ArrayList<>();
      String clusterTierManager = null;
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
              final Attr urlAttribute = ((Element)item).getAttributeNode("url");
              final String urlValue = urlAttribute.getValue();
              try {
                connectionUri = new URI(urlValue);
              } catch (URISyntaxException e) {
                throw new XmlConfigurationException(
                  String.format("Value of %s attribute on XML configuration element <%s> in <%s> is not a valid URI - '%s'",
                    urlAttribute.getName(), item.getNodeName(), fragment.getTagName(), connectionUri), e);
              }

              break;
            case "cluster-connection":
              clusterTierManager = ((Element)item).getAttribute("cluster-tier-manager");
              final NodeList serverNodes = item.getChildNodes();
              for (int j = 0; j < serverNodes.getLength(); j++) {
                final Node serverNode = serverNodes.item(j);
                final String host = ((Element)serverNode).getAttributeNode("host").getValue();
                final Attr port = ((Element)serverNode).getAttributeNode("port");
                InetSocketAddress address;
                if (port == null) {
                  address = InetSocketAddress.createUnresolved(host, 0);
                } else {
                  String portString = port.getValue();
                  address = InetSocketAddress.createUnresolved(host, Integer.parseInt(portString));
                }
                serverAddresses.add(address);
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
          if (connectionUri != null) {
            return new ClusteringServiceConfiguration(connectionUri, timeouts);
          } else {
            return new ClusteringServiceConfiguration(serverAddresses, clusterTierManager, timeouts);
          }
        }

        ServerSideConfiguration serverSideConfiguration;
        if (serverConfig.defaultServerResource == null) {
          serverSideConfiguration = new ServerSideConfiguration(serverConfig.pools);
        } else {
          serverSideConfiguration = new ServerSideConfiguration(serverConfig.defaultServerResource, serverConfig.pools);
        }

        if (connectionUri != null) {
          return new ClusteringServiceConfiguration(connectionUri, timeouts, serverConfig.autoCreate, serverSideConfiguration);
        } else {
          return new ClusteringServiceConfiguration(serverAddresses, clusterTierManager, timeouts, serverConfig.autoCreate, serverSideConfiguration);
        }
      } catch (IllegalArgumentException e) {
        throw new XmlConfigurationException(e);
      }
    }
    throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
      fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
  }

  @Override
  public Class<ClusteringService> getServiceType() {
    return ClusteringService.class;
  }

  /**
   * Translates a {@link ServiceCreationConfiguration} to an xml element
   *
   * @param serviceCreationConfiguration
   */
  @Override
  public Element unparseServiceCreationConfiguration(final ServiceCreationConfiguration<ClusteringService> serviceCreationConfiguration) {
    Element rootElement = unparseConfig(serviceCreationConfiguration);
    return rootElement;
  }

  private Element createRootUrlElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element rootElement = doc.createElementNS(getNamespace().toString(), TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTER_ELEMENT_NAME);
    Element urlElement = createUrlElement(doc, clusteringServiceConfiguration);
    rootElement.appendChild(urlElement);
    return rootElement;
  }

  protected Element createUrlElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element urlElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + CONNECTION_ELEMENT_NAME);
    urlElement.setAttribute(URL_ATTRIBUTE_NAME, clusteringServiceConfiguration.getClusterUri().toString());
    return urlElement;
  }

  private Element createServerElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    if (!(clusteringServiceConfiguration.getConnectionSource() instanceof ConnectionSource.ServerList)) {
      throw new IllegalArgumentException("When connection URL is null, source of connection MUST be of type ConnectionSource.ServerList.class");
    }
    ConnectionSource.ServerList servers = (ConnectionSource.ServerList)clusteringServiceConfiguration.getConnectionSource();
    Element rootElement = doc.createElementNS(getNamespace().toString(), TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTER_ELEMENT_NAME);
    Element connElement = createConnectionElementWrapper(doc, clusteringServiceConfiguration);
    servers.getServers().forEach(server -> {
      Element serverElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + SERVER_ELEMENT_NAME);
      serverElement.setAttribute(HOST_ATTRIBUTE_NAME, server.getHostName());
      /*
      If port is greater than 0, set the attribute. Otherwise, do not set. Default value will be taken.
       */
      if (server.getPort() > 0) {
        serverElement.setAttribute(PORT_ATTRIBUTE_NAME, Integer.toString(server.getPort()));
      }
      connElement.appendChild(serverElement);
    });
    rootElement.appendChild(connElement);
    return rootElement;
  }

  protected Element createConnectionElementWrapper(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element connElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTER_CONNECTION_ELEMENT_NAME);
    connElement.setAttribute(CLUSTER_TIER_MANAGER_ATTRIBUTE_NAME, clusteringServiceConfiguration.getConnectionSource()
      .getClusterTierManager());
    return connElement;
  }

  @Override
  protected Element createRootElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element rootElement;
    if (clusteringServiceConfiguration.getConnectionSource() instanceof ConnectionSource.ClusterUri) {
      rootElement = createRootUrlElement(doc, clusteringServiceConfiguration);
    } else {
      rootElement = createServerElement(doc, clusteringServiceConfiguration);
    }

    processTimeUnits(doc, rootElement, clusteringServiceConfiguration);
    Element serverSideConfigurationElem = processServerSideElements(doc, clusteringServiceConfiguration);
    rootElement.appendChild(serverSideConfigurationElem);
    return rootElement;
  }

  private void processTimeUnits(Document doc, Element parent, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    if (clusteringServiceConfiguration.getTimeouts() != null) {
      Timeouts timeouts = clusteringServiceConfiguration.getTimeouts();

      Element readTimeoutElem = createTimeoutElement(doc, READ_TIMEOUT_ELEMENT_NAME, timeouts.getReadOperationTimeout());
      Element writeTimeoutElem = createTimeoutElement(doc, WRITE_TIMEOUT_ELEMENT_NAME, timeouts.getWriteOperationTimeout());
      Element connectionTimeoutElem = createTimeoutElement(doc, CONNECTION_TIMEOUT_ELEMENT_NAME, timeouts.getConnectionTimeout());
      /*
      Important: do not change the order of following three elements if corresponding change is not done in xsd
       */
      parent.appendChild(readTimeoutElem);
      parent.appendChild(writeTimeoutElem);
      parent.appendChild(connectionTimeoutElem);
    }
  }

  private Element createTimeoutElement(Document doc, String timeoutName, Duration timeout) {
    Element retElement;
    if (READ_TIMEOUT_ELEMENT_NAME.equals(timeoutName)) {
      retElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + READ_TIMEOUT_ELEMENT_NAME);
    } else if (WRITE_TIMEOUT_ELEMENT_NAME.equals(timeoutName)) {
      retElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + WRITE_TIMEOUT_ELEMENT_NAME);
    } else {
      retElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + CONNECTION_TIMEOUT_ELEMENT_NAME);
    }
    retElement.setAttribute(UNIT_ATTRIBUTE_NAME, DEFAULT_UNIT_ATTRIBUTE_VALUE);
    retElement.setTextContent(Long.toString(timeout.getSeconds()));
    return retElement;
  }

  protected Element processServerSideElements(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element serverSideConfigurationElem = createServerSideConfigurationElement(doc, clusteringServiceConfiguration);

    if (clusteringServiceConfiguration.getServerConfiguration() != null) {
      ServerSideConfiguration serverSideConfiguration = clusteringServiceConfiguration.getServerConfiguration();
      String defaultServerResource = serverSideConfiguration.getDefaultServerResource();
      if (!(defaultServerResource == null || defaultServerResource.trim().length() == 0)) {
        Element defaultResourceElement = createDefaultServerResourceElement(doc, defaultServerResource);
        serverSideConfigurationElem.appendChild(defaultResourceElement);
      }
      Map<String, ServerSideConfiguration.Pool> resourcePools = serverSideConfiguration.getResourcePools();
      if (resourcePools != null) {
        resourcePools.forEach(
          (key, value) -> {
            Element poolElement = createSharedPoolElement(doc, key, value);
            serverSideConfigurationElem.appendChild(poolElement);
          }
        );
      }
    }
    return serverSideConfigurationElem;
  }

  private Element createServerSideConfigurationElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element serverSideConfigurationElem = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + SERVER_SIDE_CONFIG);
    serverSideConfigurationElem.setAttribute(AUTO_CREATE_ATTRIBUTE_NAME, Boolean.toString(clusteringServiceConfiguration
      .isAutoCreate()));
    return serverSideConfigurationElem;
  }


  private Element createSharedPoolElement(Document doc, String poolName, ServerSideConfiguration.Pool pool) {
    Element poolElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + SHARED_POOL_ELEMENT_NAME);
    poolElement.setAttribute(NAME_ATTRIBUTE_NAME, poolName);
    String from = pool.getServerResource();
    if (from != null) {
      if (from.trim().length() == 0) {
        throw new XmlConfigurationException("Resource pool name can not be empty.");
      }
      poolElement.setAttribute(FROM_ATTRIBUTE_NAME, from);
    }
    long memoryInBytes = MemoryUnit.B.convert(pool.getSize(), MemoryUnit.B);
    poolElement.setAttribute(UNIT_ATTRIBUTE_NAME, MemoryUnit.B.toString());
    poolElement.setTextContent(Long.toString(memoryInBytes));
    return poolElement;
  }

  private Element createDefaultServerResourceElement(Document doc, String defaultServerResource) {
    Element defaultResourceElement = doc.createElement(TC_CLUSTERED_NAMESPACE_PREFIX + DEFAULT_RESOURCE_ELEMENT_NAME);
    defaultResourceElement.setAttribute(FROM_ATTRIBUTE_NAME, defaultServerResource);
    return defaultResourceElement;
  }

  private ClusteringCacheManagerServiceConfigurationParser.ServerSideConfig processServerSideConfig(Node serverSideConfigElement) {
    ClusteringCacheManagerServiceConfigurationParser.ServerSideConfig serverSideConfig = new ClusteringCacheManagerServiceConfigurationParser.ServerSideConfig();
    serverSideConfig.autoCreate = Boolean.parseBoolean(((Element)serverSideConfigElement).getAttribute("auto-create"));
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

          ServerSideConfiguration.Pool poolDefinition;
          if (fromResource == null) {
            poolDefinition = new ServerSideConfiguration.Pool(memoryUnit.toBytes(quantity));
          } else {
            poolDefinition = new ServerSideConfiguration.Pool(memoryUnit.toBytes(quantity), fromResource);
          }

          if (serverSideConfig.pools.put(poolName, poolDefinition) != null) {
            throw new XmlConfigurationException("Duplicate definition for <shared-pool name=\"" + poolName + "\">");
          }
        }
      }
    }
    return serverSideConfig;
  }

  private Duration processTimeout(Element parentElement, Node timeoutNode) {
    try {
      // <xxx-timeout> are direct subtype of ehcache:time-type; use JAXB to interpret it
      JAXBContext context = JAXBContext.newInstance(TimeType.class);
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

  private Timeouts getTimeouts(Duration getTimeout, Duration putTimeout, Duration connectionTimeout) {
    TimeoutsBuilder builder = TimeoutsBuilder.timeouts();
    if (getTimeout != null) {
      builder.read(getTimeout);
    }
    if (putTimeout != null) {
      builder.write(putTimeout);
    }
    if (connectionTimeout != null) {
      builder.connection(connectionTimeout);
    }
    return builder.build();
  }

  private static final class ServerSideConfig {
    private boolean autoCreate = false;
    private String defaultServerResource = null;
    private final Map<String, ServerSideConfiguration.Pool> pools = new HashMap<>();
  }
}
