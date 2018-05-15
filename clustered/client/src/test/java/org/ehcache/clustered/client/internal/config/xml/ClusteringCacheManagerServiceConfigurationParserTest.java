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
import org.ehcache.clustered.client.config.builders.TimeoutsBuilder;
import org.ehcache.clustered.client.internal.ConnectionSource;
import org.ehcache.config.Configuration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.TimeType;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;

import static java.time.temporal.ChronoUnit.MINUTES;
import static org.ehcache.xml.XmlModel.convertToJavaTimeUnit;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ClusteringCacheManagerServiceConfigurationParserTest {

  @ClassRule
  public static final TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();

  /**
   * Ensures the {@link ClusteringCacheManagerServiceConfigurationParser} is locatable as a
   * {@link CacheManagerServiceConfigurationParser} instance.
   */
  @Test
  public void testServiceLocator() throws Exception {
    final String expectedParser = ClusteringCacheManagerServiceConfigurationParser.class.getName();
    final ServiceLoader<CacheManagerServiceConfigurationParser> parsers =
      ClassLoading.libraryServiceLoaderFor(CacheManagerServiceConfigurationParser.class);
    foundParser: {
      for (final CacheManagerServiceConfigurationParser parser : parsers) {
        if (parser.getClass().getName().equals(expectedParser)) {
          break foundParser;
        }
      }
      fail("Expected parser not found");
    }
  }

  /**
   * Ensures the namespace declared by {@link ClusteringCacheManagerServiceConfigurationParser} and its
   * schema are the same.
   */
  @Test
  public void testSchema() throws Exception {
    final ClusteringCacheManagerServiceConfigurationParser parser = new ClusteringCacheManagerServiceConfigurationParser();
    final StreamSource schemaSource = (StreamSource) parser.getXmlSchema();

    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);

    final DocumentBuilder domBuilder = factory.newDocumentBuilder();
    final Element schema = domBuilder.parse(schemaSource.getInputStream()).getDocumentElement();
    final Attr targetNamespaceAttr = schema.getAttributeNode("targetNamespace");
    assertThat(targetNamespaceAttr, is(not(nullValue())));
    assertThat(targetNamespaceAttr.getValue(), is(parser.getNamespace().toString()));
  }

  @Test
  public void testGetTimeout() throws Exception {

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\"/>",
        "      <tc:read-timeout unit=\"minutes\">5</tc:read-timeout>",
        "      <tc:write-timeout unit=\"minutes\">10</tc:write-timeout>",
        "      <tc:connection-timeout unit=\"minutes\">15</tc:connection-timeout>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));

    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations =
      configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    ClusteringServiceConfiguration clusteringServiceConfiguration =
      ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    assertThat(clusteringServiceConfiguration, is(notNullValue()));

    Timeouts timeouts = clusteringServiceConfiguration.getTimeouts();
    assertThat(timeouts.getReadOperationTimeout(), is(Duration.of(5, MINUTES)));
    assertThat(timeouts.getWriteOperationTimeout(), is(Duration.of(10, MINUTES)));
    assertThat(timeouts.getConnectionTimeout(), is(Duration.of(15, MINUTES)));
  }

  @Test
  public void testGetTimeoutNone() throws Exception {

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));

    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations =
      configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    ClusteringServiceConfiguration clusteringServiceConfiguration =
      ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    assertThat(clusteringServiceConfiguration, is(notNullValue()));

    assertThat(clusteringServiceConfiguration.getTimeouts(), is(TimeoutsBuilder.timeouts().build()));
  }

  @Test
  public void testGetTimeoutUnitDefault() throws Exception {

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\"/>",
        "      <tc:read-timeout>5</tc:read-timeout>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));

    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations =
      configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    ClusteringServiceConfiguration clusteringServiceConfiguration =
      ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    assertThat(clusteringServiceConfiguration, is(notNullValue()));

    TemporalUnit defaultUnit = convertToJavaTimeUnit(new TimeType().getUnit());
    assertThat(clusteringServiceConfiguration.getTimeouts().getReadOperationTimeout(),
      is(equalTo(Duration.of(5, defaultUnit))));
  }

  @Test
  public void testGetTimeoutUnitBad() throws Exception {

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\"/>",
        "      <tc:read-timeout unit=\"femtos\">5</tc:read-timeout>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    try {
      new XmlConfiguration(makeConfig(config));
      fail("Expecting XmlConfigurationException");
    } catch (XmlConfigurationException e) {
      assertThat(e.getMessage(), containsString("Error parsing XML configuration "));
      assertThat(e.getCause().getMessage(), containsString("Value 'femtos' is not facet-valid with respect to enumeration "));
    }
  }

  @Test
  public void testGetTimeoutValueTooBig() throws Exception {

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\"/>",
        "      <tc:read-timeout unit=\"seconds\">"
        + BigInteger.ONE.add(BigInteger.valueOf(Long.MAX_VALUE))
        + "</tc:read-timeout>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    try {
      new XmlConfiguration(makeConfig(config));
      fail("Expecting XmlConfigurationException");
    } catch (XmlConfigurationException e) {
      assertThat(e.getMessage(), containsString(" exceeds allowed value "));
    }
  }

  @Test
  public void testGetTimeoutValueOmitted() throws Exception {

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\"/>",
        "      <tc:read-timeout unit=\"seconds\"></tc:read-timeout>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    try {
      new XmlConfiguration(makeConfig(config));
      fail("Expecting XmlConfigurationException");
    } catch (XmlConfigurationException e) {
      assertThat(e.getMessage(), containsString("Error parsing XML configuration "));
      assertThat(e.getCause().getMessage(), containsString("'' is not a valid value for 'integer'"));
    }
  }

  @Test(expected = XmlConfigurationException.class)
  public void testUrlAndServers() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\" />",
        "      <tc:cluster-connection cluster-tier-manager=\"cM\">",
        "        <tc:server host=\"blah\" port=\"1234\" />",
        "      </tc:cluster-connection>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    new XmlConfiguration(makeConfig(config));
  }

  @Test(expected = XmlConfigurationException.class)
  public void testServersOnly() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:cluster-connection>",
        "        <tc:server host=\"blah\" port=\"1234\" />",
        "      </tc:cluster-connection>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    new XmlConfiguration(makeConfig(config));
  }

  @Test
  public void testServersWithClusterTierManager() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:cluster-connection cluster-tier-manager=\"cM\">",
        "        <tc:server host=\"server-1\" port=\"9510\" />",
        "        <tc:server host=\"server-2\" port=\"9540\" />",
        "      </tc:cluster-connection>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));
    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations = configuration.getServiceCreationConfigurations();
    ClusteringServiceConfiguration clusteringServiceConfiguration =
      ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    ConnectionSource.ServerList connectionSource = (ConnectionSource.ServerList) clusteringServiceConfiguration.getConnectionSource();
    Iterable<InetSocketAddress> servers = connectionSource.getServers();

    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("server-1", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 9540);
    List<InetSocketAddress> expectedServers = Arrays.asList(firstServer, secondServer);

    assertThat(connectionSource.getClusterTierManager(), is("cM"));
    assertThat(servers, is(expectedServers));
  }

  @Test
  public void testServersWithClusterTierManagerAndOptionalPorts() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:cluster-connection cluster-tier-manager=\"cM\">",
        "        <tc:server host=\"100.100.100.100\" port=\"9510\" />",
        "        <tc:server host=\"server-2\" />",
        "        <tc:server host=\"[::1]\" />",
        "        <tc:server host=\"[fe80::1453:846e:7be4:15fe]\" port=\"9710\" />",
        "      </tc:cluster-connection>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));
    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations = configuration.getServiceCreationConfigurations();
    ClusteringServiceConfiguration clusteringServiceConfiguration =
      ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    ConnectionSource.ServerList connectionSource = (ConnectionSource.ServerList)clusteringServiceConfiguration.getConnectionSource();
    Iterable<InetSocketAddress> servers = connectionSource.getServers();

    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("100.100.100.100", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 0);
    InetSocketAddress thirdServer = InetSocketAddress.createUnresolved("[::1]", 0);
    InetSocketAddress fourthServer = InetSocketAddress.createUnresolved("[fe80::1453:846e:7be4:15fe]", 9710);
    List<InetSocketAddress> expectedServers = Arrays.asList(firstServer, secondServer, thirdServer, fourthServer);

    assertThat(connectionSource.getClusterTierManager(), is("cM"));
    assertThat(servers, is(expectedServers));
  }

  @Test
  public void testTranslateServiceCreationConfiguration() throws Exception {
    URI connectionUri = new URI("terracotta://localhost:9510/my-application");
    ClusteringServiceConfiguration serviceConfig = ClusteringServiceConfigurationBuilder.cluster(connectionUri)
      .timeouts(Timeouts.DEFAULT)
      .autoCreate()
      .defaultServerResource("main")
      .resourcePool("primaryresource", 5, MemoryUnit.GB)
      .resourcePool("secondaryresource", 10, MemoryUnit.GB, "optional")
      .build();

    ClusteringCacheManagerServiceConfigurationParser parser = new ClusteringCacheManagerServiceConfigurationParser();
    Element returnElement = parser.unparseServiceCreationConfiguration(serviceConfig);

    assertThat(returnElement, is(notNullValue()));
    assertThat(returnElement.getNodeName(), is("tc:cluster"));
    assertThat(returnElement.getAttributes(), is(notNullValue()));
    assertThat(returnElement.getAttributes().getLength(), is(1));
    assertThat(returnElement.getAttributes().item(0).getNodeName(), is("xmlns:tc"));
    assertThat(returnElement.getAttributes().item(0).getNodeValue(), is("http://www.ehcache.org/v3/clustered"));
    Node connElement = returnElement.getFirstChild();
    assertThat(connElement, is(notNullValue()));
    assertThat(connElement.getNodeName(), is("tc:connection"));
    assertThat(connElement.getAttributes(), is(notNullValue()));
    assertThat(connElement.getAttributes().getLength(), is(1));
    assertThat(connElement.getAttributes().item(0), is(notNullValue()));
    assertThat(connElement.getAttributes().item(0).getNodeName(), is("url"));
    assertThat(connElement.getAttributes().item(0).getNodeValue(), is("terracotta://localhost:9510/my-application"));
    Node readTomeOut = connElement.getNextSibling();
    assertThat(readTomeOut, is(notNullValue()));
    assertThat(readTomeOut.getNodeName(), is(notNullValue()));
    assertThat(readTomeOut.getNodeName(), is("tc:read-timeout"));
    assertThat(readTomeOut.getAttributes(), is(notNullValue()));
    assertThat(readTomeOut.getAttributes().getLength(), is(1));
    assertThat(readTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(readTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(readTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));

    Node writeTomeOut = readTomeOut.getNextSibling();

    assertThat(writeTomeOut, is(notNullValue()));
    assertThat(writeTomeOut.getNodeName(), is(notNullValue()));
    assertThat(writeTomeOut.getNodeName(), is("tc:write-timeout"));
    assertThat(writeTomeOut.getAttributes(), is(notNullValue()));
    assertThat(writeTomeOut.getAttributes().getLength(), is(1));
    assertThat(writeTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(writeTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(writeTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));
    assertThat(writeTomeOut.getFirstChild().getNodeValue(), is("5"));

    Node connectionTomeOut = writeTomeOut.getNextSibling();

    assertThat(connectionTomeOut, is(notNullValue()));
    assertThat(connectionTomeOut.getNodeName(), is(notNullValue()));
    assertThat(connectionTomeOut.getNodeName(), is("tc:connection-timeout"));
    assertThat(connectionTomeOut.getAttributes(), is(notNullValue()));
    assertThat(connectionTomeOut.getAttributes().getLength(), is(1));
    assertThat(connectionTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(connectionTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(connectionTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));
    assertThat(connectionTomeOut.getFirstChild().getNodeValue(), is("150"));

    Node serverSideConfig = connectionTomeOut.getNextSibling();

    assertThat(serverSideConfig, is(notNullValue()));
    assertThat(serverSideConfig.getNodeName(), is(notNullValue()));
    assertThat(serverSideConfig.getNodeName(), is("tc:server-side-config"));
    assertThat(serverSideConfig.getAttributes(), is(notNullValue()));
    assertThat(serverSideConfig.getAttributes().getLength(), is(1));
    assertThat(serverSideConfig.getAttributes().item(0), is(notNullValue()));
    assertThat(serverSideConfig.getAttributes().item(0).getNodeName(), is("auto-create"));
    assertThat(serverSideConfig.getAttributes().item(0).getNodeValue(), is("true"));

    Node defaultResource = serverSideConfig.getFirstChild();

    assertThat(defaultResource, is(notNullValue()));
    assertThat(defaultResource.getNodeName(), is(notNullValue()));
    assertThat(defaultResource.getNodeName(), is("tc:default-resource"));
    assertThat(defaultResource.getAttributes(), is(notNullValue()));
    assertThat(defaultResource.getAttributes().getLength(), is(1));
    assertThat(defaultResource.getAttributes().item(0), is(notNullValue()));
    assertThat(defaultResource.getAttributes().item(0).getNodeName(), is("from"));
    assertThat(defaultResource.getAttributes().item(0).getNodeValue(), is("main"));

    Node sharedPool = defaultResource.getNextSibling();

    assertThat(sharedPool, is(notNullValue()));
    assertThat(sharedPool.getNodeName(), is(notNullValue()));
    assertThat(sharedPool.getNodeName(), is("tc:shared-pool"));
    assertThat(sharedPool.getAttributes(), is(notNullValue()));
    assertThat(sharedPool.getAttributes().getLength(), greaterThan(1));

    SharePoolAttributesAndValueHolder attributeValoeHolder1 = getSharedPoolAttributes(sharedPool);

    assertThat("primaryresource".equals(attributeValoeHolder1.name) || "secondaryresource".equals(attributeValoeHolder1.name), is(true));

    if ("primaryresource".equals(attributeValoeHolder1.name)) {
      assertThat(attributeValoeHolder1.memoryUnit, is(equalTo("B")));
      assertThat(attributeValoeHolder1.memoryValue, is(equalTo(5368709120L)));
      assertThat(attributeValoeHolder1.from, is(nullValue()));
    } else {
      assertThat(attributeValoeHolder1.memoryUnit, is(equalTo("B")));
      assertThat(attributeValoeHolder1.memoryValue, is(equalTo(10737418240L)));
      assertThat(attributeValoeHolder1.from, is(notNullValue()));
      assertThat(attributeValoeHolder1.from, is(equalTo("optional")));
    }

    sharedPool = sharedPool.getNextSibling();
    assertThat(sharedPool, is(notNullValue()));
    assertThat(sharedPool.getNodeName(), is(notNullValue()));
    assertThat(sharedPool.getNodeName(), is("tc:shared-pool"));
    assertThat(sharedPool.getAttributes(), is(notNullValue()));
    assertThat(sharedPool.getAttributes().getLength(), greaterThan(1));

    SharePoolAttributesAndValueHolder attributeValoeHolder2 = getSharedPoolAttributes(sharedPool);

    assertThat("primaryresource".equals(attributeValoeHolder2.name) || "secondaryresource".equals(attributeValoeHolder2.name), is(true));

    assertThat((("primaryresource".equals(attributeValoeHolder1.name) && "secondaryresource".equals(attributeValoeHolder2.name))
                || ("secondaryresource".equals(attributeValoeHolder1.name) && "primaryresource".equals(attributeValoeHolder2.name))), is(true));

    if ("primaryresource".equals(attributeValoeHolder2.name)) {
      assertThat(attributeValoeHolder2.memoryUnit, is(equalTo("B")));
      assertThat(attributeValoeHolder2.memoryValue, is(equalTo(5368709120L)));
      assertThat(attributeValoeHolder2.from, is(nullValue()));
    } else {
      assertThat(attributeValoeHolder2.memoryUnit, is(equalTo("B")));
      assertThat(attributeValoeHolder2.memoryValue, is(equalTo(10737418240L)));
      assertThat(attributeValoeHolder2.from, is(notNullValue()));
      assertThat(attributeValoeHolder2.from, is(equalTo("optional")));
    }
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithNoResourcePoolAndAutoCreateFalse() throws Exception {
    URI connectionUri = new URI("terracotta://localhost:9510/my-application");
    ClusteringServiceConfiguration serviceConfig = ClusteringServiceConfigurationBuilder.cluster(connectionUri)
      .timeouts(Timeouts.DEFAULT)
      .expecting()
      .defaultServerResource("main")
      .build();


    ClusteringCacheManagerServiceConfigurationParser parser = new ClusteringCacheManagerServiceConfigurationParser();
    Element returnElement = parser.unparseServiceCreationConfiguration(serviceConfig);


    assertThat(returnElement, is(notNullValue()));
    assertThat(returnElement.getNodeName(), is("tc:cluster"));
    assertThat(returnElement.getAttributes(), is(notNullValue()));
    assertThat(returnElement.getAttributes().getLength(), is(1));
    assertThat(returnElement.getAttributes().item(0).getNodeName(), is("xmlns:tc"));
    assertThat(returnElement.getAttributes().item(0).getNodeValue(), is("http://www.ehcache.org/v3/clustered"));
    Node connElement = returnElement.getFirstChild();
    assertThat(connElement, is(notNullValue()));
    assertThat(connElement.getNodeName(), is("tc:connection"));
    assertThat(connElement.getAttributes(), is(notNullValue()));
    assertThat(connElement.getAttributes().getLength(), is(1));
    assertThat(connElement.getAttributes().item(0), is(notNullValue()));
    assertThat(connElement.getAttributes().item(0).getNodeName(), is("url"));
    assertThat(connElement.getAttributes().item(0).getNodeValue(), is("terracotta://localhost:9510/my-application"));
    Node readTomeOut = connElement.getNextSibling();
    assertThat(readTomeOut, is(notNullValue()));
    assertThat(readTomeOut.getNodeName(), is(notNullValue()));
    assertThat(readTomeOut.getNodeName(), is("tc:read-timeout"));
    assertThat(readTomeOut.getAttributes(), is(notNullValue()));
    assertThat(readTomeOut.getAttributes().getLength(), is(1));
    assertThat(readTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(readTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(readTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));

    Node writeTomeOut = readTomeOut.getNextSibling();

    assertThat(writeTomeOut, is(notNullValue()));
    assertThat(writeTomeOut.getNodeName(), is(notNullValue()));
    assertThat(writeTomeOut.getNodeName(), is("tc:write-timeout"));
    assertThat(writeTomeOut.getAttributes(), is(notNullValue()));
    assertThat(writeTomeOut.getAttributes().getLength(), is(1));
    assertThat(writeTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(writeTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(writeTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));
    assertThat(writeTomeOut.getFirstChild().getNodeValue(), is("5"));

    Node connectionTomeOut = writeTomeOut.getNextSibling();

    assertThat(connectionTomeOut, is(notNullValue()));
    assertThat(connectionTomeOut.getNodeName(), is(notNullValue()));
    assertThat(connectionTomeOut.getNodeName(), is("tc:connection-timeout"));
    assertThat(connectionTomeOut.getAttributes(), is(notNullValue()));
    assertThat(connectionTomeOut.getAttributes().getLength(), is(1));
    assertThat(connectionTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(connectionTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(connectionTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));
    assertThat(connectionTomeOut.getFirstChild().getNodeValue(), is("150"));

    Node serverSideConfig = connectionTomeOut.getNextSibling();

    assertThat(serverSideConfig, is(notNullValue()));
    assertThat(serverSideConfig.getNodeName(), is(notNullValue()));
    assertThat(serverSideConfig.getNodeName(), is("tc:server-side-config"));
    assertThat(serverSideConfig.getAttributes(), is(notNullValue()));
    assertThat(serverSideConfig.getAttributes().getLength(), is(1));
    assertThat(serverSideConfig.getAttributes().item(0), is(notNullValue()));
    assertThat(serverSideConfig.getAttributes().item(0).getNodeName(), is("auto-create"));
    assertThat(serverSideConfig.getAttributes().item(0).getNodeValue(), is("false"));

    Node defaultResource = serverSideConfig.getFirstChild();

    assertThat(defaultResource, is(notNullValue()));
    assertThat(defaultResource.getNodeName(), is(notNullValue()));
    assertThat(defaultResource.getNodeName(), is("tc:default-resource"));
    assertThat(defaultResource.getAttributes(), is(notNullValue()));
    assertThat(defaultResource.getAttributes().getLength(), is(1));
    assertThat(defaultResource.getAttributes().item(0), is(notNullValue()));
    assertThat(defaultResource.getAttributes().item(0).getNodeName(), is("from"));
    assertThat(defaultResource.getAttributes().item(0).getNodeValue(), is("main"));

    Node sharedPool = defaultResource.getNextSibling();

    assertThat(sharedPool, is(nullValue()));
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithNoServerSideConfig() throws Exception {
    URI connectionUri = new URI("terracotta://localhost:9510/my-application");
    ClusteringServiceConfiguration serviceConfig = ClusteringServiceConfigurationBuilder.cluster(connectionUri)
      .timeouts(Timeouts.DEFAULT)
      .build();

    ClusteringCacheManagerServiceConfigurationParser parser = new ClusteringCacheManagerServiceConfigurationParser();
    Element returnElement = parser.unparseServiceCreationConfiguration(serviceConfig);

    assertThat(returnElement, is(notNullValue()));
    assertThat(returnElement.getNodeName(), is("tc:cluster"));
    assertThat(returnElement.getAttributes(), is(notNullValue()));
    assertThat(returnElement.getAttributes().getLength(), is(1));
    assertThat(returnElement.getAttributes().item(0).getNodeName(), is("xmlns:tc"));
    assertThat(returnElement.getAttributes().item(0).getNodeValue(), is("http://www.ehcache.org/v3/clustered"));
    Node connElement = returnElement.getFirstChild();
    assertThat(connElement, is(notNullValue()));
    assertThat(connElement.getNodeName(), is("tc:connection"));
    assertThat(connElement.getAttributes(), is(notNullValue()));
    assertThat(connElement.getAttributes().getLength(), is(1));
    assertThat(connElement.getAttributes().item(0), is(notNullValue()));
    assertThat(connElement.getAttributes().item(0).getNodeName(), is("url"));
    assertThat(connElement.getAttributes().item(0).getNodeValue(), is("terracotta://localhost:9510/my-application"));
    Node readTomeOut = connElement.getNextSibling();
    assertThat(readTomeOut, is(notNullValue()));
    assertThat(readTomeOut.getNodeName(), is(notNullValue()));
    assertThat(readTomeOut.getNodeName(), is("tc:read-timeout"));
    assertThat(readTomeOut.getAttributes(), is(notNullValue()));
    assertThat(readTomeOut.getAttributes().getLength(), is(1));
    assertThat(readTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(readTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(readTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));

    Node writeTomeOut = readTomeOut.getNextSibling();

    assertThat(writeTomeOut, is(notNullValue()));
    assertThat(writeTomeOut.getNodeName(), is(notNullValue()));
    assertThat(writeTomeOut.getNodeName(), is("tc:write-timeout"));
    assertThat(writeTomeOut.getAttributes(), is(notNullValue()));
    assertThat(writeTomeOut.getAttributes().getLength(), is(1));
    assertThat(writeTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(writeTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(writeTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));
    assertThat(writeTomeOut.getFirstChild().getNodeValue(), is("5"));

    Node connectionTomeOut = writeTomeOut.getNextSibling();

    assertThat(connectionTomeOut, is(notNullValue()));
    assertThat(connectionTomeOut.getNodeName(), is(notNullValue()));
    assertThat(connectionTomeOut.getNodeName(), is("tc:connection-timeout"));
    assertThat(connectionTomeOut.getAttributes(), is(notNullValue()));
    assertThat(connectionTomeOut.getAttributes().getLength(), is(1));
    assertThat(connectionTomeOut.getAttributes().item(0), is(notNullValue()));
    assertThat(connectionTomeOut.getAttributes().item(0).getNodeName(), is("unit"));
    assertThat(connectionTomeOut.getAttributes().item(0).getNodeValue(), is("seconds"));
    assertThat(connectionTomeOut.getFirstChild().getNodeValue(), is("150"));

    Node serverSideConfig = connectionTomeOut.getNextSibling();

    assertThat(serverSideConfig, is(notNullValue()));
    assertThat(serverSideConfig.getNodeName(), is(notNullValue()));
    assertThat(serverSideConfig.getNodeName(), is("tc:server-side-config"));
    assertThat(serverSideConfig.getAttributes(), is(notNullValue()));
    assertThat(serverSideConfig.getAttributes().getLength(), is(1));
    assertThat(serverSideConfig.getAttributes().item(0), is(notNullValue()));
    assertThat(serverSideConfig.getAttributes().item(0).getNodeName(), is("auto-create"));
    assertThat(serverSideConfig.getAttributes().item(0).getNodeValue(), is("false"));

    Node defaultResource = serverSideConfig.getFirstChild();

    assertThat(defaultResource, is(nullValue()));
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithInetSocketAddress() throws Exception {

    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("100.100.100.100", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 0);
    InetSocketAddress thirdServer = InetSocketAddress.createUnresolved("[::1]", 0);
    InetSocketAddress fourthServer = InetSocketAddress.createUnresolved("[fe80::1453:846e:7be4:15fe]", 9710);
    List<InetSocketAddress> servers = Arrays.asList(firstServer, secondServer, thirdServer, fourthServer);
    ClusteringServiceConfiguration serviceConfig = ClusteringServiceConfigurationBuilder.cluster(servers, "my-application")
      .timeouts(Timeouts.DEFAULT)
      .build();

    ClusteringCacheManagerServiceConfigurationParser parser = new ClusteringCacheManagerServiceConfigurationParser();
    Element returnElement = parser.unparseServiceCreationConfiguration(serviceConfig);

    assertThat(returnElement, is(notNullValue()));
    assertThat(returnElement.getNodeName(), is("tc:cluster"));
    assertThat(returnElement.getAttributes(), is(notNullValue()));
    assertThat(returnElement.getAttributes().getLength(), is(1));
    assertThat(returnElement.getAttributes().item(0).getNodeName(), is("xmlns:tc"));
    assertThat(returnElement.getAttributes().item(0).getNodeValue(), is("http://www.ehcache.org/v3/clustered"));
    Node connElement = returnElement.getFirstChild();
    assertThat(connElement, is(notNullValue()));
    assertThat(connElement.getNodeName(), is("tc:cluster-connection"));
    assertThat(connElement.getAttributes(), is(notNullValue()));

    int numberOfServers = 0;

    Node serverElement = connElement.getFirstChild();
    assertThat(serverElement, is(notNullValue()));
    assertThat(serverElement.getNodeName(), is("tc:server"));
    assertThat(connElement.getAttributes(), is(notNullValue()));

    do {
      numberOfServers++;
      InetSocketAddress retSocketAddress = extractInetSocketAddressFromServerElement(serverElement);
      assertThat(servers.contains(retSocketAddress), is(true));
      serverElement = serverElement.getNextSibling();
    } while (serverElement != null);
    assertThat(numberOfServers, is(equalTo(4)));
  }

  /**
   * Constructs a temporary XML configuration file.
   *
   * @param lines the lines to include in the XML configuration file
   * @return a {@code URL} pointing to the XML configuration file
   * @throws IOException if an error is raised while creating or writing the XML configuration file
   */
  @SuppressWarnings("ThrowFromFinallyBlock")
  private URL makeConfig(final String[] lines) throws IOException {
    final File configFile = folder.newFile(testName.getMethodName() + "_config.xml");

    OutputStreamWriter out = null;
    try {
      out = new OutputStreamWriter(new FileOutputStream(configFile), "UTF-8");
      for (final String line : lines) {
        out.write(line);
      }
    } finally {
      if (out != null) {
        try {
          out.close();
        } catch (IOException e) {
          throw new AssertionError(e);
        }
      }
    }

    return configFile.toURI().toURL();
  }

  private SharePoolAttributesAndValueHolder getSharedPoolAttributes(Node sharedPool) {
    SharePoolAttributesAndValueHolder attributeHolder = new SharePoolAttributesAndValueHolder();
    attributeHolder.memoryValue = Long.parseLong(sharedPool.getFirstChild().getNodeValue());
    for (int index = 0; index < sharedPool.getAttributes().getLength(); index++) {
      Node attribute = sharedPool.getAttributes().item(index);
      assertThat(attribute, is(notNullValue()));
      String nodeName = attribute.getNodeName();
      assertThat(nodeName, is(notNullValue()));
      if (nodeName.equals("name")) {
        String poolName;
        poolName = attribute.getNodeValue();
        attributeHolder.name = poolName;

      } else if (nodeName.equals("unit")) {
        attributeHolder.memoryUnit = attribute.getNodeValue();
      } else if (nodeName.equals("from")) {
        attributeHolder.from = attribute.getNodeValue();
      }
    }
    return attributeHolder;
  }

  private InetSocketAddress extractInetSocketAddressFromServerElement(Node serverElement) {
    NamedNodeMap attributeMap = serverElement.getAttributes();
    String host = null;
    String port = null;
    for (int i = 0; i < attributeMap.getLength(); i++) {
      Node attribute = attributeMap.item(i);
      if ("host".equals(attribute.getNodeName())) {
        host = attribute.getNodeValue();
      }
      if ("port".equals(attribute.getNodeName())) {
        port = attribute.getNodeValue();
      }
    }
    int portNumber = 0;
    if (port != null) {
      portNumber = Integer.parseInt(port);
    }

    return InetSocketAddress.createUnresolved(host, portNumber);
  }

  private static final class SharePoolAttributesAndValueHolder {
    private String name;
    private long memoryValue;
    private String memoryUnit;
    private String from;
  }
}
