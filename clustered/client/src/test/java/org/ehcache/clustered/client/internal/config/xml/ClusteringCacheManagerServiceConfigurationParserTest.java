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
import org.ehcache.clustered.common.ServerSideConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.TimeTypeWithPropSubst;
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;

import static java.time.temporal.ChronoUnit.MINUTES;
import static java.util.Spliterators.spliterator;
import static java.util.stream.StreamSupport.stream;
import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.ehcache.xml.XmlConfigurationMatchers.isSameConfigurationAs;
import static org.ehcache.xml.XmlModel.convertToJavaTimeUnit;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.junit.Assert.fail;

public class ClusteringCacheManagerServiceConfigurationParserTest {

  @ClassRule
  public static final TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();

  private static final String PROPERTY_PREFIX = ClusteringCacheManagerServiceConfigurationParserTest.class.getName() + ":";

  /**
   * Ensures the {@link ClusteringCacheManagerServiceConfigurationParser} is locatable as a
   * {@link CacheManagerServiceConfigurationParser} instance.
   */
  @Test
  public void testServiceLocator() throws Exception {
    assertThat(stream(spliterator(ClassLoading.servicesOfType(CacheManagerServiceConfigurationParser.class).iterator(), Long.MAX_VALUE, 0), false).map(Object::getClass).collect(Collectors.toList()),
      hasItem(ClusteringCacheManagerServiceConfigurationParser.class));
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

    Collection<ServiceCreationConfiguration<?, ?>> serviceCreationConfigurations =
      configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    ClusteringServiceConfiguration clusteringServiceConfiguration =
      findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
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

    Collection<ServiceCreationConfiguration<?, ?>> serviceCreationConfigurations =
      configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    ClusteringServiceConfiguration clusteringServiceConfiguration =
      findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
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

    Collection<ServiceCreationConfiguration<?, ?>> serviceCreationConfigurations =
      configuration.getServiceCreationConfigurations();
    assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

    ClusteringServiceConfiguration clusteringServiceConfiguration =
      findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
    assertThat(clusteringServiceConfiguration, is(notNullValue()));

    TemporalUnit defaultUnit = convertToJavaTimeUnit(new TimeTypeWithPropSubst().getUnit());
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
      assertThat(e.getCause().getMessage(), allOf(containsString("facet"), containsString("enumeration"), containsString("femtos")));
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
      assertThat(e.getCause().getMessage(), allOf(containsString("propertyOrPositiveInteger"), containsString("valid"), containsString("not")));
    }
  }

  @Test
  public void testGetTimeoutAsProperty() throws Exception {
    String readTimeoutProperty = PROPERTY_PREFIX + testName.getMethodName() + ":read";
    String writeTimeoutProperty = PROPERTY_PREFIX + testName.getMethodName() + ":write";
    String connectTimeoutProperty = PROPERTY_PREFIX + testName.getMethodName() + ":connect";
    Map<String, String> properties = new HashMap<>();
    properties.put(readTimeoutProperty, "5");
    properties.put(writeTimeoutProperty, "10");
    properties.put(connectTimeoutProperty, "15");

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\"/>",
        "      <tc:read-timeout unit=\"minutes\">${" + readTimeoutProperty + "}</tc:read-timeout>",
        "      <tc:write-timeout unit=\"minutes\">${" + writeTimeoutProperty + "}</tc:write-timeout>",
        "      <tc:connection-timeout unit=\"minutes\">${" + connectTimeoutProperty + "}</tc:connection-timeout>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    properties.forEach(System::setProperty);
    try {
      final Configuration configuration = new XmlConfiguration(makeConfig(config));

      Collection<ServiceCreationConfiguration<?, ?>> serviceCreationConfigurations =
        configuration.getServiceCreationConfigurations();
      assertThat(serviceCreationConfigurations, is(not(Matchers.empty())));

      ClusteringServiceConfiguration clusteringServiceConfiguration =
        findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
      assertThat(clusteringServiceConfiguration, is(notNullValue()));

      Timeouts timeouts = clusteringServiceConfiguration.getTimeouts();
      assertThat(timeouts.getReadOperationTimeout(), is(Duration.of(5, MINUTES)));
      assertThat(timeouts.getWriteOperationTimeout(), is(Duration.of(10, MINUTES)));
      assertThat(timeouts.getConnectionTimeout(), is(Duration.of(15, MINUTES)));
    } finally {
      properties.keySet().forEach(System::clearProperty);
    }
  }

  @Test
  public void testUrlWithProperty() throws Exception {
    String serverProperty = PROPERTY_PREFIX + testName.getMethodName() + ":server";
    String portProperty = PROPERTY_PREFIX + testName.getMethodName() + ":port";
    Map<String, String> properties = new HashMap<>();
    properties.put(serverProperty, "example.com");
    properties.put(portProperty, "9540");

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://${" + serverProperty + "}:${" + portProperty + "}/cachemanager\" />",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    properties.forEach(System::setProperty);
    try {
      XmlConfiguration configuration = new XmlConfiguration(makeConfig(config));
      ClusteringServiceConfiguration clusteringConfig = findSingletonAmongst(ClusteringServiceConfiguration.class, configuration.getServiceCreationConfigurations());
      ConnectionSource.ClusterUri connectionSource = (ConnectionSource.ClusterUri) clusteringConfig.getConnectionSource();
      assertThat(connectionSource.getClusterUri(), is(URI.create("terracotta://example.com:9540/cachemanager")));
    } finally {
      properties.keySet().forEach(System::clearProperty);
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
    Collection<ServiceCreationConfiguration<?, ?>> serviceCreationConfigurations = configuration.getServiceCreationConfigurations();
    ClusteringServiceConfiguration clusteringServiceConfiguration =
      findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
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
    Collection<ServiceCreationConfiguration<?, ?>> serviceCreationConfigurations = configuration.getServiceCreationConfigurations();
    ClusteringServiceConfiguration clusteringServiceConfiguration =
      findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
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
  public void testServersWithClusterTierManagerAndOptionalPortsUsingProperties() throws Exception {
    String hostProperty = PROPERTY_PREFIX + testName.getMethodName() + ":host";
    String portProperty = PROPERTY_PREFIX + testName.getMethodName() + ":port";
    String tierManagerProperty = PROPERTY_PREFIX + testName.getMethodName() + ":tierManager";
    Map<String, String> properties = new HashMap<>();
    properties.put(hostProperty, "100.100.100.100");
    properties.put(portProperty, "9510");
    properties.put(tierManagerProperty, "george");

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:cluster-connection cluster-tier-manager='${" + tierManagerProperty + "}'>",
        "        <tc:server host='${" + hostProperty + "}' port='${" + portProperty + "}'/>",
        "      </tc:cluster-connection>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    properties.forEach(System::setProperty);
    try {
      final Configuration configuration = new XmlConfiguration(makeConfig(config));
      Collection<ServiceCreationConfiguration<?, ?>> serviceCreationConfigurations = configuration.getServiceCreationConfigurations();
      ClusteringServiceConfiguration clusteringServiceConfiguration =
        findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);
      ConnectionSource.ServerList connectionSource = (ConnectionSource.ServerList) clusteringServiceConfiguration.getConnectionSource();
      Iterable<InetSocketAddress> servers = connectionSource.getServers();

      assertThat(connectionSource.getClusterTierManager(), is("george"));
      assertThat(servers, contains(InetSocketAddress.createUnresolved("100.100.100.100", 9510)));
    } finally {
      properties.keySet().forEach(System::clearProperty);
    }
  }

  @Test @SuppressWarnings("deprecation")
  public void testAutoCreateFalseMapsToExpecting() throws IOException {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\" />",
        "      <tc:server-side-config auto-create=\"false\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "</ehcache:config>"
      };

    XmlConfiguration configuration = new XmlConfiguration(makeConfig(config));
    ClusteringServiceConfiguration clusterConfig = findSingletonAmongst(ClusteringServiceConfiguration.class, configuration.getServiceCreationConfigurations());

    assertThat(clusterConfig.isAutoCreate(), is(false));
    assertThat(clusterConfig.getClientMode(), is(ClusteringServiceConfiguration.ClientMode.EXPECTING));
  }

  @Test @SuppressWarnings("deprecation")
  public void testAutoCreateTrueMapsToAutoCreate() throws IOException {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\" />",
        "      <tc:server-side-config auto-create=\"true\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "</ehcache:config>"
      };

    XmlConfiguration configuration = new XmlConfiguration(makeConfig(config));
    ClusteringServiceConfiguration clusterConfig = findSingletonAmongst(ClusteringServiceConfiguration.class, configuration.getServiceCreationConfigurations());

    assertThat(clusterConfig.isAutoCreate(), is(true));
    assertThat(clusterConfig.getClientMode(), is(ClusteringServiceConfiguration.ClientMode.AUTO_CREATE));
  }

  @Test
  public void testBothAutoCreateAndClientModeIsDisallowed() throws IOException {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\" />",
        "      <tc:server-side-config auto-create=\"true\" client-mode=\"auto-create\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "</ehcache:config>"
      };

    try {
      new XmlConfiguration(makeConfig(config));
    } catch (XmlConfigurationException e) {
      assertThat(e.getMessage(), is("Cannot define both 'auto-create' and 'client-mode' attributes"));
    }
  }

  @Test
  public void testClientModeAsAProperty() throws IOException {
    String clientModeProperty = PROPERTY_PREFIX + testName.getMethodName() + ":client-mode";
    Map<String, String> properties = new HashMap<>();
    properties.put(clientModeProperty, "auto-create");

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\" />",
        "      <tc:server-side-config client-mode='${" + clientModeProperty + "}'/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "</ehcache:config>"
      };

    properties.forEach(System::setProperty);
    try {
      XmlConfiguration configuration = new XmlConfiguration(makeConfig(config));
      ClusteringServiceConfiguration clusterConfig = findSingletonAmongst(ClusteringServiceConfiguration.class, configuration.getServiceCreationConfigurations());
      assertThat(clusterConfig.getClientMode(), is(ClusteringServiceConfiguration.ClientMode.AUTO_CREATE));
    } finally {
      properties.keySet().forEach(System::clearProperty);
    }
  }

  @Test
  public void testSharedPoolUsingProperties() throws IOException {
    String poolSizeProperty = PROPERTY_PREFIX + testName.getMethodName() + ":pool-size";
    String fromProperty = PROPERTY_PREFIX + testName.getMethodName() + ":from";
    Map<String, String> properties = new HashMap<>();
    properties.put(poolSizeProperty, "1024");
    properties.put(fromProperty, "source");

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url='terracotta://example.com:9540/cachemanager'/>",
        "      <tc:server-side-config client-mode='auto-create'>",
        "        <tc:shared-pool name='pool' from='${" + fromProperty + "}'>",
        "          ${" + poolSizeProperty + "}",
        "        </tc:shared-pool>",
        "      </tc:server-side-config>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "</ehcache:config>"
      };

    properties.forEach(System::setProperty);
    try {
      XmlConfiguration configuration = new XmlConfiguration(makeConfig(config));
      ClusteringServiceConfiguration clusterConfig = findSingletonAmongst(ClusteringServiceConfiguration.class, configuration.getServiceCreationConfigurations());
      ServerSideConfiguration.Pool pool = clusterConfig.getServerConfiguration().getResourcePools().get("pool");
      assertThat(pool.getSize(), is(1024L));
      assertThat(pool.getServerResource(), is("source"));
    } finally {
      properties.keySet().forEach(System::clearProperty);
    }
  }

  @Test
  public void testDefaultResourceAsAProperty() throws IOException {
    String fromProperty = PROPERTY_PREFIX + testName.getMethodName() + ":from";
    Map<String, String> properties = new HashMap<>();
    properties.put(fromProperty, "source");

    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url='terracotta://example.com:9540/cachemanager'/>",
        "      <tc:server-side-config client-mode='auto-create'>",
        "        <tc:default-resource from='${" + fromProperty + "}'/>",
        "      </tc:server-side-config>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "</ehcache:config>"
      };

    properties.forEach(System::setProperty);
    try {
      XmlConfiguration configuration = new XmlConfiguration(makeConfig(config));
      ClusteringServiceConfiguration clusterConfig = findSingletonAmongst(ClusteringServiceConfiguration.class, configuration.getServiceCreationConfigurations());
      assertThat(clusterConfig.getServerConfiguration().getDefaultServerResource(), is("source"));
    } finally {
      properties.keySet().forEach(System::clearProperty);
    }
  }

  @Test
  public void testTranslateServiceCreationConfiguration() throws Exception {
    URI connectionUri = new URI("terracotta://localhost:9510/my-application");
    ClusteringServiceConfiguration serviceConfig = ClusteringServiceConfigurationBuilder.cluster(connectionUri)
      .timeouts(Timeouts.DEFAULT)
      .autoCreate(server -> server
        .defaultServerResource("main")
        .resourcePool("primaryresource", 5, MemoryUnit.GB)
        .resourcePool("secondaryresource", 10, MemoryUnit.GB, "optional"))
      .build();

    ClusteringCacheManagerServiceConfigurationParser parser = new ClusteringCacheManagerServiceConfigurationParser();
    Element returnElement = parser.unparseServiceCreationConfiguration(serviceConfig);

    String inputString = "<tc:cluster xmlns:tc = \"http://www.ehcache.org/v3/clustered\">" +
                         "<tc:connection url = \"terracotta://localhost:9510/my-application\"/>" +
                         "<tc:read-timeout unit = \"seconds\">5</tc:read-timeout>" +
                         "<tc:write-timeout unit = \"seconds\">5</tc:write-timeout>" +
                         "<tc:connection-timeout unit = \"seconds\">150</tc:connection-timeout>" +
                         "<tc:server-side-config client-mode = \"auto-create\">" +
                         "<tc:default-resource from = \"main\"/>" +
                         "<tc:shared-pool name = \"primaryresource\" unit = \"B\">5368709120</tc:shared-pool>" +
                         "<tc:shared-pool from = \"optional\" name = \"secondaryresource\" unit = \"B\">10737418240</tc:shared-pool>" +
                         "</tc:server-side-config></tc:cluster>";
    assertThat(returnElement, isSameConfigurationAs(inputString));
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithNoResourcePoolAndAutoCreateFalse() throws Exception {
    URI connectionUri = new URI("terracotta://localhost:9510/my-application");
    ClusteringServiceConfiguration serviceConfig = ClusteringServiceConfigurationBuilder.cluster(connectionUri)
      .timeouts(Timeouts.DEFAULT)
      .expecting(server -> server.defaultServerResource("main"))
      .build();


    ClusteringCacheManagerServiceConfigurationParser parser = new ClusteringCacheManagerServiceConfigurationParser();
    Element returnElement = parser.unparseServiceCreationConfiguration(serviceConfig);

    String inputString = "<tc:cluster xmlns:tc = \"http://www.ehcache.org/v3/clustered\">" +
                         "<tc:connection url = \"terracotta://localhost:9510/my-application\"/>" +
                         "<tc:read-timeout unit = \"seconds\">5</tc:read-timeout>" +
                         "<tc:write-timeout unit = \"seconds\">5</tc:write-timeout>" +
                         "<tc:connection-timeout unit = \"seconds\">150</tc:connection-timeout>" +
                         "<tc:server-side-config client-mode = \"expecting\">" +
                         "<tc:default-resource from = \"main\"/>" +
                         "</tc:server-side-config></tc:cluster>";
    assertThat(returnElement, isSameConfigurationAs(inputString));
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithNoServerSideConfig() throws Exception {
    URI connectionUri = new URI("terracotta://localhost:9510/my-application");
    ClusteringServiceConfiguration serviceConfig = ClusteringServiceConfigurationBuilder.cluster(connectionUri)
      .timeouts(Timeouts.DEFAULT)
      .build();

    ClusteringCacheManagerServiceConfigurationParser parser = new ClusteringCacheManagerServiceConfigurationParser();
    Element returnElement = parser.unparseServiceCreationConfiguration(serviceConfig);

    String inputString = "<tc:cluster xmlns:tc = \"http://www.ehcache.org/v3/clustered\">" +
                         "<tc:connection url = \"terracotta://localhost:9510/my-application\"/>" +
                         "<tc:read-timeout unit = \"seconds\">5</tc:read-timeout>" +
                         "<tc:write-timeout unit = \"seconds\">5</tc:write-timeout>" +
                         "<tc:connection-timeout unit = \"seconds\">150</tc:connection-timeout>" +
                         "</tc:cluster>";
    assertThat(returnElement, isSameConfigurationAs(inputString));
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithInetSocketAddress() {

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

    String inputString = "<tc:cluster xmlns:tc = \"http://www.ehcache.org/v3/clustered\">" +
                         "<tc:cluster-connection cluster-tier-manager = \"my-application\">" +
                         "<tc:server host = \"100.100.100.100\" port = \"9510\"/>" +
                         "<tc:server host = \"server-2\"/>" +
                         "<tc:server host = \"[::1]\"/>" +
                         "<tc:server host = \"[fe80::1453:846e:7be4:15fe]\" port = \"9710\"/>" +
                         "</tc:cluster-connection>" +
                         "<tc:read-timeout unit = \"seconds\">5</tc:read-timeout>" +
                         "<tc:write-timeout unit = \"seconds\">5</tc:write-timeout>" +
                         "<tc:connection-timeout unit = \"seconds\">150</tc:connection-timeout>" +
                         "</tc:cluster>";
    assertThat(returnElement, isSameConfigurationAs(inputString));
  }

  /**
   * Constructs a temporary XML configuration file.
   *
   * @param lines the lines to include in the XML configuration file
   * @return a {@code URL} pointing to the XML configuration file
   * @throws IOException if an error is raised while creating or writing the XML configuration file
   */
  private URL makeConfig(final String[] lines) throws IOException {
    final File configFile = folder.newFile(testName.getMethodName() + "_config.xml");

    try (FileOutputStream fout = new FileOutputStream(configFile); OutputStreamWriter out = new OutputStreamWriter(fout, StandardCharsets.UTF_8)) {
      for (final String line : lines) {
        out.write(line);
      }
    }

    return configFile.toURI().toURL();
  }

}
