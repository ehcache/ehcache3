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
import org.ehcache.config.Configuration;
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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.net.InetSocketAddress;
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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Basic tests for {@link ClusteringServiceConfigurationParser}.
 */
public class ClusteringServiceConfigurationParserTest {

  @ClassRule
  public static final TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();


  /**
   * Ensures the {@link ClusteringServiceConfigurationParser} is locatable as a
   * {@link CacheManagerServiceConfigurationParser} instance.
   */
  @Test
  public void testServiceLocator() throws Exception {
    final String expectedParser = ClusteringServiceConfigurationParser.class.getName();
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
   * Ensures the namespace declared by {@link ClusteringServiceConfigurationParser} and its
   * schema are the same.
   */
  @Test
  public void testSchema() throws Exception {
    final ClusteringServiceConfigurationParser parser = new ClusteringServiceConfigurationParser();
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

  @Test
  public void testUrlAndServers() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\" servers=\"example.com:9540\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };
    try {
      new XmlConfiguration(makeConfig(config));
      fail("Expected XmlConfigurationException");
    } catch (XmlConfigurationException e) {
      assertThat(e.getMessage(), containsString("Either of 'url' or 'servers' must be specified, not both"));
    }
  }

  @Test
  public void testUrlAndCacheManager() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection url=\"terracotta://example.com:9540/cachemanager\" cacheManager=\"cM\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };
    try {
      new XmlConfiguration(makeConfig(config));
      fail("Expected XmlConfigurationException");
    } catch (XmlConfigurationException e) {
      assertThat(e.getMessage(), containsString("'cacheManager' attribute can only be used with 'servers' attribute"));
    }
  }

  @Test
  public void testServersOnly() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection servers=\"server-1:9540 server-2:9540\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };
    try {
      new XmlConfiguration(makeConfig(config));
      fail("Expected XmlConfigurationException");
    } catch (XmlConfigurationException e) {
      assertThat(e.getMessage(), containsString("'cacheManager' attribute needs to be specified with 'servers' attribute"));
    }
  }

  @Test
  public void testServersWithCacheManager() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection servers=\"server-1:9510 server-2:9540\" cacheManager=\"cM\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));
    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations =
        configuration.getServiceCreationConfigurations();

    ClusteringServiceConfiguration clusteringServiceConfiguration =
        ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);

    assertThat(clusteringServiceConfiguration.getCacheManagerName(), is("cM"));
    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("server-1", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 9540);
    Iterable<InetSocketAddress> servers = clusteringServiceConfiguration.getServers();
    List<InetSocketAddress> expectedServers = Arrays.asList(firstServer, secondServer);
    assertThat(servers, is(expectedServers));
  }

  @Test
  public void testServersWithCacheManagerAndOptionalPorts() throws Exception {
    final String[] config = new String[]
      {
        "<ehcache:config",
        "    xmlns:ehcache=\"http://www.ehcache.org/v3\"",
        "    xmlns:tc=\"http://www.ehcache.org/v3/clustered\">",
        "",
        "  <ehcache:service>",
        "    <tc:cluster>",
        "      <tc:connection servers=\"100.100.100.100:9510 server-2 [::1] [::2]:9710\" cacheManager=\"cM\"/>",
        "    </tc:cluster>",
        "  </ehcache:service>",
        "",
        "</ehcache:config>"
      };

    final Configuration configuration = new XmlConfiguration(makeConfig(config));
    Collection<ServiceCreationConfiguration<?>> serviceCreationConfigurations =
        configuration.getServiceCreationConfigurations();

    ClusteringServiceConfiguration clusteringServiceConfiguration =
        ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, serviceCreationConfigurations);

    assertThat(clusteringServiceConfiguration.getCacheManagerName(), is("cM"));
    InetSocketAddress firstServer = InetSocketAddress.createUnresolved("100.100.100.100", 9510);
    InetSocketAddress secondServer = InetSocketAddress.createUnresolved("server-2", 0);
    InetSocketAddress thirdServer = InetSocketAddress.createUnresolved("[::1]", 0);
    InetSocketAddress fourthServer = InetSocketAddress.createUnresolved("[::2]", 9710);
    Iterable<InetSocketAddress> servers = clusteringServiceConfiguration.getServers();
    List<InetSocketAddress> expectedServers = Arrays.asList(firstServer, secondServer, thirdServer, fourthServer);
    assertThat(servers, is(expectedServers));
  }

  /**
   * Constructs a temporary XML configuration file.
   *
   * @param lines the lines to include in the XML configuration file
   *
   * @return a {@code URL} pointing to the XML configuration file
   *
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
}
