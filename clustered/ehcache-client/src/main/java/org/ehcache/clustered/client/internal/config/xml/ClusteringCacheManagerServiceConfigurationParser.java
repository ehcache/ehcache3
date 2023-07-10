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
import org.ehcache.clustered.client.config.builders.ClusteringServiceConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.ServerSideConfigurationBuilder;
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.ConnectionSource;
import org.ehcache.clustered.client.service.ClusteringService;
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.config.Builder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.spi.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.TypeInfo;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static java.lang.String.format;
import static org.ehcache.xml.spi.ParsingUtil.parsePropertyOrPositiveInteger;
import static org.ehcache.xml.spi.ParsingUtil.parsePropertyOrString;
import static org.ehcache.xml.spi.ParsingUtil.parseStringWithProperties;

/**
 * Provides parsing support for the {@code <service>} elements representing a {@link ClusteringService ClusteringService}.
 */
@Component
public class ClusteringCacheManagerServiceConfigurationParser extends ClusteringParser<ClusteringServiceConfiguration>
  implements CacheManagerServiceConfigurationParser<ClusteringService, ClusteringServiceConfiguration> {

  public static final String CONNECTION_ELEMENT_NAME = "connection";
  public static final String CLUSTER_CONNECTION_ELEMENT_NAME = "cluster-connection";
  public static final String SERVER_SIDE_CONFIG_ELEMENT_NAME = "server-side-config";

  private static final String CLUSTER_ELEMENT_NAME = "cluster";
  private static final String CLUSTER_TIER_MANAGER_ATTRIBUTE_NAME = "cluster-tier-manager";
  private static final String SERVER_ELEMENT_NAME = "server";
  private static final String HOST_ATTRIBUTE_NAME = "host";
  private static final String PORT_ATTRIBUTE_NAME = "port";
  private static final String READ_TIMEOUT_ELEMENT_NAME = "read-timeout";
  private static final String WRITE_TIMEOUT_ELEMENT_NAME = "write-timeout";
  private static final String CONNECTION_TIMEOUT_ELEMENT_NAME = "connection-timeout";
  private static final String URL_ATTRIBUTE_NAME = "url";
  private static final String DEFAULT_RESOURCE_ELEMENT_NAME = "default-resource";
  private static final String SHARED_POOL_ELEMENT_NAME = "shared-pool";
  private static final String AUTO_CREATE_ATTRIBUTE_NAME = "auto-create";
  private static final String CLIENT_MODE_ATTRIBUTE_NAME = "client-mode";
  private static final String UNIT_ATTRIBUTE_NAME = "unit";
  private static final String NAME_ATTRIBUTE_NAME = "name";
  private static final String FROM_ATTRIBUTE_NAME = "from";
  private static final String DEFAULT_UNIT_ATTRIBUTE_VALUE = "seconds";

  /**
   * Complete interpretation of the top-level elements defined in <code>{@link ClusteringParser#NAMESPACE}</code>.
   * This method is called only for those elements from that namespace.
   * <p>
   * This method presumes the element presented is valid according to the XSD.
   *
   * @param fragment the XML fragment to process
   * @param classLoader
   * @return a {@link org.ehcache.clustered.client.config.ClusteringServiceConfiguration ClusteringServiceConfiguration}
   */
  @Override
  public ClusteringServiceConfiguration parse(final Element fragment, ClassLoader classLoader) {
    if (CLUSTER_ELEMENT_NAME.equals(fragment.getLocalName())) {
      return parseServerBehavior(parseConnection(fragment)
        .timeouts(parseTimeouts(fragment)), fragment).build();
    } else {
      throw new XmlConfigurationException(format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
    }
  }

  protected ClusteringServiceConfigurationBuilder parseConnection(Element cluster) {
    Optional<Element> connectionElement = childElementOf(cluster, element ->
      (element.getNamespaceURI().equals(NAMESPACE) && element.getLocalName().equals(CONNECTION_ELEMENT_NAME))
        || element.getSchemaTypeInfo().isDerivedFrom(NAMESPACE, "connection-spec", TypeInfo.DERIVATION_EXTENSION)
    );
    Optional<Element> clusterConnectionElement = childElementOf(cluster, element ->
      (element.getNamespaceURI().equals(NAMESPACE) && element.getLocalName().equals(CLUSTER_CONNECTION_ELEMENT_NAME))
        || element.getSchemaTypeInfo().isDerivedFrom(NAMESPACE, "cluster-connection-spec", TypeInfo.DERIVATION_EXTENSION)
    );

    if (clusterConnectionElement.isPresent()) {
      Element clusterConnection = clusterConnectionElement.get();
      String clusterTierManager = parsePropertyOrString(clusterConnection.getAttribute(CLUSTER_TIER_MANAGER_ATTRIBUTE_NAME));
      List<InetSocketAddress> serverAddresses = new ArrayList<>();
      final NodeList serverNodes = clusterConnection.getElementsByTagNameNS(NAMESPACE, SERVER_ELEMENT_NAME);
      for (int j = 0; j < serverNodes.getLength(); j++) {
        final Node serverNode = serverNodes.item(j);
        final String host = parsePropertyOrString(((Element) serverNode).getAttributeNode(HOST_ATTRIBUTE_NAME).getValue().trim());
        final Attr port = ((Element) serverNode).getAttributeNode(PORT_ATTRIBUTE_NAME);
        InetSocketAddress address;
        if (port == null) {
          address = InetSocketAddress.createUnresolved(host, 0);
        } else {
          String portString = parsePropertyOrString(port.getValue());
          address = InetSocketAddress.createUnresolved(host, Integer.parseInt(portString));
        }
        serverAddresses.add(address);
      }
      return ClusteringServiceConfigurationBuilder.cluster(serverAddresses, clusterTierManager);
    } else if (connectionElement.isPresent()) {
      Element connection = connectionElement.get();
      final Attr urlAttribute = connection.getAttributeNode(URL_ATTRIBUTE_NAME);
      final String urlValue = parseStringWithProperties(urlAttribute.getValue());
      try {
        return ClusteringServiceConfigurationBuilder.cluster(new URI(urlValue));
      } catch (URISyntaxException e) {
        throw new XmlConfigurationException(format("Value of %s attribute on XML configuration element <%s> in <%s> is not a valid URI - '%s'",
          urlAttribute.getName(), connection.getNodeName(), connection.getParentNode().getNodeName(), urlValue), e);
      }
    } else {
      throw new AssertionError("Validation Leak!");
    }
  }

  protected Builder<? extends Timeouts> parseTimeouts(Element cluster) {
    NodeList childNodes = cluster.getChildNodes();
    TimeoutsBuilder timeoutsBuilder = TimeoutsBuilder.timeouts();
    for (int i = 0; i < childNodes.getLength(); i++) {
      final Node item = childNodes.item(i);
      if (Node.ELEMENT_NODE == item.getNodeType()) {
        switch (item.getLocalName()) {
          case READ_TIMEOUT_ELEMENT_NAME:
            /*
             * <read-timeout> is an optional element
             */
            timeoutsBuilder.read(processTimeout((Element) item));
            break;
          case WRITE_TIMEOUT_ELEMENT_NAME:
            /*
             * <write-timeout> is an optional element
             */
            timeoutsBuilder.write(processTimeout((Element) item));
            break;
          case CONNECTION_TIMEOUT_ELEMENT_NAME:
            /*
             * <connection-timeout> is an optional element
             */
            timeoutsBuilder.connection(processTimeout((Element) item));
            break;
          default:
            //skip
        }
      }
    }
    return timeoutsBuilder;
  }

  protected ClusteringServiceConfigurationBuilder parseServerBehavior(ClusteringServiceConfigurationBuilder builder, Element cluster) {
    Optional<Element> serverSideConfigElement = childElementOf(cluster, element ->
      (element.getNamespaceURI().equals(NAMESPACE) && element.getLocalName().equals(SERVER_SIDE_CONFIG_ELEMENT_NAME))
        || element.getSchemaTypeInfo().isDerivedFrom(NAMESPACE, "server-side-config-spec", TypeInfo.DERIVATION_EXTENSION)
    );

    if (serverSideConfigElement.isPresent()) {
      Element serverSideConfig = serverSideConfigElement.get();
      String autoCreateAttr = serverSideConfig.getAttribute(AUTO_CREATE_ATTRIBUTE_NAME);
      String clientModeAttr = serverSideConfig.getAttribute(CLIENT_MODE_ATTRIBUTE_NAME);

      if (clientModeAttr.isEmpty()) {
        if (!autoCreateAttr.isEmpty()) {
          if (Boolean.parseBoolean(autoCreateAttr)) {
            return builder.autoCreate(parseServerConfig(serverSideConfig));
          } else {
            return builder.expecting(parseServerConfig(serverSideConfig));
          }
        } else {
          return builder;
        }
      } else if (autoCreateAttr.isEmpty()) {
        clientModeAttr = parsePropertyOrString(clientModeAttr);
        switch (clientModeAttr) {
          case "expecting":
            return builder.expecting(parseServerConfig(serverSideConfig));
          case "auto-create":
            return builder.autoCreate(parseServerConfig(serverSideConfig));
          case "auto-create-on-reconnect":
            return builder.autoCreateOnReconnect(parseServerConfig(serverSideConfig));
          default:
            throw new AssertionError("Validation Leak!");
        }
      } else {
        throw new XmlConfigurationException("Cannot define both '" + AUTO_CREATE_ATTRIBUTE_NAME + "' and '" + CLIENT_MODE_ATTRIBUTE_NAME + "' attributes");
      }
    } else {
      return builder;
    }
  }

  private UnaryOperator<ServerSideConfigurationBuilder> parseServerConfig(Element serverSideConfig) {
    Function<ServerSideConfigurationBuilder, ServerSideConfigurationBuilder> builder = Function.identity();


    Optional<Element> defaultResourceElement = optionalSingleton(Element.class, serverSideConfig.getElementsByTagNameNS(NAMESPACE, DEFAULT_RESOURCE_ELEMENT_NAME));

    if (defaultResourceElement.isPresent()) {
      String defaultServerResource = parsePropertyOrString(defaultResourceElement.get().getAttribute(FROM_ATTRIBUTE_NAME));
      builder = builder.andThen(b -> b.defaultServerResource(defaultServerResource));
    }

    NodeList sharedPoolElements = serverSideConfig.getElementsByTagNameNS(NAMESPACE, SHARED_POOL_ELEMENT_NAME);
    for (int i = 0; i < sharedPoolElements.getLength(); i++) {
      final Element pool = (Element) sharedPoolElements.item(i);
      String poolName = pool.getAttribute(NAME_ATTRIBUTE_NAME);     // required
      Attr fromAttr = pool.getAttributeNode(FROM_ATTRIBUTE_NAME);   // optional
      String fromResource = (fromAttr == null ? null : fromAttr.getValue());
      Attr unitAttr = pool.getAttributeNode(UNIT_ATTRIBUTE_NAME);   // optional - default 'B'
      String unit = (unitAttr == null ? "B" : unitAttr.getValue());
      MemoryUnit memoryUnit = MemoryUnit.valueOf(unit.toUpperCase(Locale.ENGLISH));

      String quantityValue = pool.getTextContent();
      long quantity;
      try {
        quantity = parsePropertyOrPositiveInteger(quantityValue).longValueExact();
      } catch (NumberFormatException e) {
        throw new XmlConfigurationException("Magnitude of value specified for <shared-pool name=\""
          + poolName + "\"> is too large");
      }

      if (fromResource == null) {
        builder = builder.andThen(b -> b.resourcePool(poolName, quantity, memoryUnit));
      } else {
        builder = builder.andThen(b -> b.resourcePool(poolName, quantity, memoryUnit, parsePropertyOrString(fromResource)));
      }
    }

    return builder::apply;
  }

  @Override
  public Class<ClusteringService> getServiceType() {
    return ClusteringService.class;
  }

  private Element createRootUrlElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element rootElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTER_ELEMENT_NAME);
    Element urlElement = createUrlElement(doc, clusteringServiceConfiguration);
    rootElement.appendChild(urlElement);
    return rootElement;
  }

  protected Element createUrlElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element urlElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + CONNECTION_ELEMENT_NAME);
    urlElement.setAttribute(URL_ATTRIBUTE_NAME, clusteringServiceConfiguration.getClusterUri().toString());
    return urlElement;
  }

  private Element createServerElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    if (!(clusteringServiceConfiguration.getConnectionSource() instanceof ConnectionSource.ServerList)) {
      throw new IllegalArgumentException("When connection URL is null, source of connection MUST be of type ConnectionSource.ServerList.class");
    }
    ConnectionSource.ServerList servers = (ConnectionSource.ServerList)clusteringServiceConfiguration.getConnectionSource();
    Element rootElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTER_ELEMENT_NAME);
    Element connElement = createConnectionElementWrapper(doc, clusteringServiceConfiguration);
    servers.getServers().forEach(server -> {
      Element serverElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + SERVER_ELEMENT_NAME);
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
    Element connElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTER_CONNECTION_ELEMENT_NAME);
    connElement.setAttribute(CLUSTER_TIER_MANAGER_ATTRIBUTE_NAME, clusteringServiceConfiguration.getConnectionSource()
      .getClusterTierManager());
    return connElement;
  }

  @Override
  public Element safeUnparse(Document target, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element rootElement;
    if (clusteringServiceConfiguration.getConnectionSource() instanceof ConnectionSource.ClusterUri) {
      rootElement = createRootUrlElement(target, clusteringServiceConfiguration);
    } else {
      rootElement = createServerElement(target, clusteringServiceConfiguration);
    }

    processTimeUnits(target, rootElement, clusteringServiceConfiguration);
    Element serverSideConfigurationElem = processServerSideElements(target, clusteringServiceConfiguration);
    if (serverSideConfigurationElem != null) {
      rootElement.appendChild(serverSideConfigurationElem);
    }
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
      retElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + READ_TIMEOUT_ELEMENT_NAME);
    } else if (WRITE_TIMEOUT_ELEMENT_NAME.equals(timeoutName)) {
      retElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + WRITE_TIMEOUT_ELEMENT_NAME);
    } else {
      retElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + CONNECTION_TIMEOUT_ELEMENT_NAME);
    }
    retElement.setAttribute(UNIT_ATTRIBUTE_NAME, DEFAULT_UNIT_ATTRIBUTE_VALUE);
    retElement.setTextContent(Long.toString(timeout.getSeconds()));
    return retElement;
  }

  protected Element processServerSideElements(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    switch (clusteringServiceConfiguration.getClientMode()) {
      case CONNECT:
        return null;
      case EXPECTING:
      case AUTO_CREATE:
      case AUTO_CREATE_ON_RECONNECT:
        Element serverSideConfigurationElem = createServerSideConfigurationElement(doc, clusteringServiceConfiguration);
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
        return serverSideConfigurationElem;
    }
    throw new AssertionError();
  }

  private Element createServerSideConfigurationElement(Document doc, ClusteringServiceConfiguration clusteringServiceConfiguration) {
    Element serverSideConfigurationElem = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + SERVER_SIDE_CONFIG_ELEMENT_NAME);
    serverSideConfigurationElem.setAttribute(CLIENT_MODE_ATTRIBUTE_NAME, clusteringServiceConfiguration.getClientMode()
      .name().toLowerCase(Locale.ROOT).replace('_', '-'));
    return serverSideConfigurationElem;
  }


  private Element createSharedPoolElement(Document doc, String poolName, ServerSideConfiguration.Pool pool) {
    Element poolElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + SHARED_POOL_ELEMENT_NAME);
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
    Element defaultResourceElement = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + DEFAULT_RESOURCE_ELEMENT_NAME);
    defaultResourceElement.setAttribute(FROM_ATTRIBUTE_NAME, defaultServerResource);
    return defaultResourceElement;
  }

  private Duration processTimeout(Element timeoutNode) {
    BigInteger amount = parsePropertyOrPositiveInteger(timeoutNode.getTextContent());
    if (amount.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
      throw new XmlConfigurationException(
        String.format("Value of XML configuration element <%s> in <%s> exceeds allowed value - %s",
          timeoutNode.getNodeName(), timeoutNode.getParentNode().getNodeName(), amount));
    }

    Attr unitAttribute = timeoutNode.getAttributeNode("unit");
    if (unitAttribute == null) {
      return Duration.of(amount.longValue(), ChronoUnit.SECONDS);
    } else {
      return Duration.of(amount.longValue(), ChronoUnit.valueOf(unitAttribute.getValue().toUpperCase(Locale.ROOT)));
    }
  }
}
