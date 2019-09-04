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
package org.ehcache.xml.multi;

import org.ehcache.config.Configuration;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.XmlConfigurationTest;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.junit.Test;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.DefaultNodeMatcher;

import java.net.URISyntaxException;
import java.net.URL;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.hamcrest.collection.IsIterableContainingInAnyOrder.containsInAnyOrder;
import static org.hamcrest.collection.IsMapContaining.hasKey;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.hamcrest.core.IsNull.nullValue;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.xmlunit.diff.ElementSelectors.and;
import static org.xmlunit.diff.ElementSelectors.byNameAndAllAttributes;
import static org.xmlunit.diff.ElementSelectors.byNameAndText;
import static org.xmlunit.matchers.CompareMatcher.isSimilarTo;

public class XmlMultiConfigurationTest {

  @Test
  public void testEmptyConfigurationFromBuilder() {
    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.fromNothing().build();

    assertThat(xmlMultiConfiguration.configuration("foo"), nullValue());
    assertThat(xmlMultiConfiguration.configuration("foo", "prod"), nullValue());

    assertThat(xmlMultiConfiguration.identities(), empty());
    assertThrows(() -> xmlMultiConfiguration.variants("foo"), IllegalArgumentException.class);

    assertThat(xmlMultiConfiguration.toString(),
      isSimilarTo("<configurations xmlns='http://www.ehcache.org/v3/multi'/>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testSimpleConfigurationBuiltFromEmpty() {
    Configuration config = emptyConfiguration();

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.fromNothing().withManager("foo", config).build();

    assertThat(xmlMultiConfiguration.configuration("foo"), sameInstance(config));
    assertThat(xmlMultiConfiguration.configuration("foo", "prod"), sameInstance(config));

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());

    assertThat(xmlMultiConfiguration.toString(),
      isSimilarTo(
        "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
          "<configuration identity='foo'><config xmlns='http://www.ehcache.org/v3'/></configuration>" +
        "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testVariantConfigurationBuiltFromEmpty() {
    Configuration barVariant = emptyConfiguration();
    Configuration bazVariant = emptyConfiguration();
    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.fromNothing()
      .withManager("foo").variant("bar", barVariant).variant("baz", bazVariant).build();

    assertThrows(() -> xmlMultiConfiguration.configuration("foo"), IllegalStateException.class);

    assertThat(xmlMultiConfiguration.configuration("foo", "bar"), sameInstance(barVariant));
    assertThat(xmlMultiConfiguration.configuration("foo", "baz"), sameInstance(bazVariant));

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo"));
    assertThat(xmlMultiConfiguration.variants("foo"), containsInAnyOrder("bar", "baz"));

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='foo'>" +
          "<variant type='bar'><config xmlns='http://www.ehcache.org/v3'/></variant>" +
          "<variant type='baz'><config xmlns='http://www.ehcache.org/v3'/></variant>" +
        "</configuration>" +
      "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testMixedConfigurationBuiltFromEmpty() {
    Configuration barVariant = emptyConfiguration();
    Configuration bazVariant = emptyConfiguration();
    Configuration fiiConfig = emptyConfiguration();
    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.fromNothing()
      .withManager("foo").variant("bar", barVariant).variant("baz", bazVariant)
      .withManager("fum").variant("bar", barVariant)
      .withManager("fii", fiiConfig).build();

    assertThrows(() -> xmlMultiConfiguration.configuration("foo"), IllegalStateException.class);

    assertThat(xmlMultiConfiguration.configuration("foo", "bar"), sameInstance(barVariant));
    assertThat(xmlMultiConfiguration.configuration("foo", "baz"), sameInstance(bazVariant));
    assertThat(xmlMultiConfiguration.configuration("fum", "bar"), sameInstance(barVariant));
    assertThat(xmlMultiConfiguration.configuration("fii", "bar"), sameInstance(fiiConfig));
    assertThat(xmlMultiConfiguration.configuration("fii", "baz"), sameInstance(fiiConfig));
    assertThat(xmlMultiConfiguration.configuration("fii"), sameInstance(fiiConfig));

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo", "fii", "fum"));
    assertThat(xmlMultiConfiguration.variants("foo"), containsInAnyOrder("bar", "baz"));
    assertThat(xmlMultiConfiguration.variants("fum"), containsInAnyOrder("bar"));
    assertThat(xmlMultiConfiguration.variants("fii"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='foo'>" +
          "<variant type='bar'><config xmlns='http://www.ehcache.org/v3'/></variant>" +
          "<variant type='baz'><config xmlns='http://www.ehcache.org/v3'/></variant>" +
        "</configuration>" +
        "<configuration identity='fum'>" +
          "<variant type='bar'><config xmlns='http://www.ehcache.org/v3'/></variant>" +
        "</configuration>" +
        "<configuration identity='fii'>" +
          "<config xmlns='http://www.ehcache.org/v3'/>" +
        "</configuration>" +
      "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testEmptyConfigurationFromXml() throws URISyntaxException {
    URL resource = XmlConfigurationTest.class.getResource("/configs/multi/empty.xml");

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(resource).build();

    assertThat(xmlMultiConfiguration.configuration("foo"), nullValue());
    assertThat(xmlMultiConfiguration.configuration("foo", "prod"), nullValue());

    assertThat(xmlMultiConfiguration.identities(), empty());
    assertThrows(() -> xmlMultiConfiguration.variants("foo"), IllegalArgumentException.class);

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(Input.fromURI(resource.toURI())).ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testMultipleConfigurationsFromXml() throws URISyntaxException {
    URL resource = XmlConfigurationTest.class.getResource("/configs/multi/multiple-configs.xml");

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(resource).build();

    assertThat(xmlMultiConfiguration.configuration("foo").getCacheConfigurations(), hasKey("foo"));
    assertThat(xmlMultiConfiguration.configuration("foo", "prod").getCacheConfigurations(), hasKey("foo"));
    assertThat(xmlMultiConfiguration.configuration("bar").getCacheConfigurations(), hasKey("bar"));
    assertThat(xmlMultiConfiguration.configuration("bar", "prod").getCacheConfigurations(), hasKey("bar"));

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo", "bar"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());
    assertThat(xmlMultiConfiguration.variants("bar"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(Input.fromURI(resource.toURI())).ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testMultipleVariantsFromXml() throws URISyntaxException {
    URL resource = XmlConfigurationTest.class.getResource("/configs/multi/multiple-variants.xml");

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(resource).build();

    assertThrows(() -> xmlMultiConfiguration.configuration("foo"), IllegalStateException.class);

    assertThat(xmlMultiConfiguration.configuration("foo", "development").getCacheConfigurations(), hasKey("foo-dev"));
    assertThat(xmlMultiConfiguration.configuration("foo", "production").getCacheConfigurations(), hasKey("foo-prod"));
    assertThat(xmlMultiConfiguration.configuration("bar", "development").getCacheConfigurations(), hasKey("bar-dev"));
    assertThat(xmlMultiConfiguration.configuration("bar", "production").getCacheConfigurations(), hasKey("bar-prod"));

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo", "bar"));
    assertThat(xmlMultiConfiguration.variants("foo"), containsInAnyOrder("development", "production"));
    assertThat(xmlMultiConfiguration.variants("bar"), containsInAnyOrder("development", "production"));

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(Input.fromURI(resource.toURI())).ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testManagerRemovedFromXml() throws URISyntaxException {
    URL resource = XmlConfigurationTest.class.getResource("/configs/multi/multiple-configs.xml");

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(resource).withoutManager("bar").build();

    assertThat(xmlMultiConfiguration.configuration("foo").getCacheConfigurations(), hasKey("foo"));
    assertThat(xmlMultiConfiguration.configuration("foo", "prod").getCacheConfigurations(), hasKey("foo"));
    assertThat(xmlMultiConfiguration.configuration("bar"), nullValue());
    assertThat(xmlMultiConfiguration.configuration("bar", "prod"), nullValue());

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='foo'>" +
          "<ehcache:config xmlns:ehcache='http://www.ehcache.org/v3' xmlns:multi='http://www.ehcache.org/v3/multi' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>" +
            "<ehcache:cache alias='foo'><ehcache:heap unit='entries'>100</ehcache:heap></ehcache:cache>" +
          "</ehcache:config>" +
        "</configuration>" +
      "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testManagerRemovedFromXmlAndReadded() throws URISyntaxException {
    URL resource = XmlConfigurationTest.class.getResource("/configs/multi/multiple-configs.xml");

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(resource).withoutManager("bar").withManager("bar", emptyConfiguration()).build();

    assertThat(xmlMultiConfiguration.configuration("foo").getCacheConfigurations(), hasKey("foo"));
    assertThat(xmlMultiConfiguration.configuration("foo", "prod").getCacheConfigurations(), hasKey("foo"));
    assertThat(xmlMultiConfiguration.configuration("bar"), notNullValue());
    assertThat(xmlMultiConfiguration.configuration("bar", "prod"), notNullValue());

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo", "bar"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());
    assertThat(xmlMultiConfiguration.variants("bar"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='bar'>" +
          "<config xmlns='http://www.ehcache.org/v3'/>" +
        "</configuration>" +
        "<configuration identity='foo'>" +
          "<ehcache:config xmlns:ehcache='http://www.ehcache.org/v3' xmlns:multi='http://www.ehcache.org/v3/multi' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>" +
            "<ehcache:cache alias='foo'><ehcache:heap unit='entries'>100</ehcache:heap></ehcache:cache>" +
          "</ehcache:config>" +
        "</configuration>" +
      "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testManagerAddedToXml() throws URISyntaxException {
    URL resource = XmlConfigurationTest.class.getResource("/configs/multi/multiple-configs.xml");

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(resource).withManager("baz", emptyConfiguration()).build();

    assertThat(xmlMultiConfiguration.configuration("foo").getCacheConfigurations(), hasKey("foo"));
    assertThat(xmlMultiConfiguration.configuration("foo", "prod").getCacheConfigurations(), hasKey("foo"));
    assertThat(xmlMultiConfiguration.configuration("bar").getCacheConfigurations(), hasKey("bar"));
    assertThat(xmlMultiConfiguration.configuration("bar", "prod").getCacheConfigurations(), hasKey("bar"));
    assertThat(xmlMultiConfiguration.configuration("baz"), notNullValue());
    assertThat(xmlMultiConfiguration.configuration("baz", "prod"), notNullValue());

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo", "bar", "baz"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());
    assertThat(xmlMultiConfiguration.variants("bar"), empty());
    assertThat(xmlMultiConfiguration.variants("baz"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='bar'>" +
          "<ehcache:config xmlns:ehcache='http://www.ehcache.org/v3' xmlns:multi='http://www.ehcache.org/v3/multi' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>" +
            "<ehcache:cache alias='bar'><ehcache:heap unit='entries'>100</ehcache:heap></ehcache:cache>" +
          "</ehcache:config>" +
        "</configuration>" +
        "<configuration identity='baz'>" +
          "<config xmlns='http://www.ehcache.org/v3'/>" +
        "</configuration>" +
        "<configuration identity='foo'>" +
          "<ehcache:config xmlns:ehcache='http://www.ehcache.org/v3' xmlns:multi='http://www.ehcache.org/v3/multi' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'>" +
            "<ehcache:cache alias='foo'><ehcache:heap unit='entries'>100</ehcache:heap></ehcache:cache>" +
          "</ehcache:config>" +
        "</configuration>" +
      "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testManagerRemovedFromConfig() throws URISyntaxException {
    XmlMultiConfiguration source = XmlMultiConfiguration.fromNothing().withManager("foo", emptyConfiguration()).build();

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(source).withoutManager("foo").build();

    assertThat(xmlMultiConfiguration.configuration("foo"), nullValue());
    assertThat(xmlMultiConfiguration.configuration("foo", "prod"), nullValue());

    assertThat(xmlMultiConfiguration.identities(), empty());
    assertThrows(() -> xmlMultiConfiguration.variants("foo"), IllegalArgumentException.class);

    assertThat(xmlMultiConfiguration.toString(),
      isSimilarTo("<configurations xmlns='http://www.ehcache.org/v3/multi'></configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testManagerRemovedFromConfigAndReadded() throws URISyntaxException {
    XmlMultiConfiguration source = XmlMultiConfiguration.fromNothing().withManager("foo", emptyConfiguration()).build();

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(source).withoutManager("foo").withManager("foo", emptyConfiguration()).build();

    assertThat(xmlMultiConfiguration.configuration("foo"), notNullValue());
    assertThat(xmlMultiConfiguration.configuration("foo", "prod"), notNullValue());

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='foo'><config xmlns='http://www.ehcache.org/v3'/></configuration>" +
      "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testManagerAddedToConfig() throws URISyntaxException {
    XmlMultiConfiguration source = XmlMultiConfiguration.fromNothing().withManager("foo", emptyConfiguration()).build();

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(source).withManager("baz", emptyConfiguration()).build();

    assertThat(xmlMultiConfiguration.configuration("foo"), notNullValue());
    assertThat(xmlMultiConfiguration.configuration("foo", "prod"), notNullValue());
    assertThat(xmlMultiConfiguration.configuration("baz"), notNullValue());
    assertThat(xmlMultiConfiguration.configuration("baz", "prod"), notNullValue());

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo", "baz"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());
    assertThat(xmlMultiConfiguration.variants("baz"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='foo'><config xmlns='http://www.ehcache.org/v3'/></configuration>" +
        "<configuration identity='baz'><config xmlns='http://www.ehcache.org/v3'/></configuration>" +
      "</configurations>").ignoreWhitespace().ignoreComments().withNodeMatcher(
      new DefaultNodeMatcher(and(byNameAndText, byNameAndAllAttributes))));
  }

  @Test
  public void testGenerateExtendedConfiguration() throws URISyntaxException {
    XmlConfiguration extended = new XmlConfiguration(getClass().getResource("/configs/all-extensions.xml"));

    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.fromNothing().withManager("foo", extended).build();

    assertThat(xmlMultiConfiguration.configuration("foo"), sameInstance(extended));

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='foo'>" +
          "<config xmlns='http://www.ehcache.org/v3' xmlns:bar='http://www.example.com/bar' xmlns:baz='http://www.example.com/baz' xmlns:foo='http://www.example.com/foo' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.example.com/baz baz.xsd http://www.ehcache.org/v3 ../../../main/resources/ehcache-core.xsd'>" +
            "<service><bar:bar/></service>" +
            "<cache alias='fancy'>" +
              "<key-type>java.lang.String</key-type>" +
              "<value-type>java.lang.String</value-type>" +
              "<resources>" +
                "<heap unit='entries'>10</heap>" +
                "<baz:baz/>" +
              "</resources>" +
              "<foo:foo/>" +
            "</cache>" +
          "</config>" +
        "</configuration>" +
      "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test
  public void testParseExtendedConfiguration() {
    XmlMultiConfiguration xmlMultiConfiguration = XmlMultiConfiguration.from(getClass().getResource("/configs/multi/extended.xml")).build();

    assertThat(xmlMultiConfiguration.configuration("foo"), notNullValue());

    assertThat(xmlMultiConfiguration.identities(), containsInAnyOrder("foo"));
    assertThat(xmlMultiConfiguration.variants("foo"), empty());

    assertThat(xmlMultiConfiguration.toString(), isSimilarTo(
      "<configurations xmlns='http://www.ehcache.org/v3/multi'>" +
        "<configuration identity='foo'>" +
        "<config xmlns='http://www.ehcache.org/v3' xmlns:bar='http://www.example.com/bar' xmlns:baz='http://www.example.com/baz' xmlns:foo='http://www.example.com/foo' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.example.com/baz baz.xsd http://www.ehcache.org/v3 ../../../main/resources/ehcache-core.xsd'>" +
        "<service><bar:bar/></service>" +
        "<cache alias='fancy'>" +
        "<key-type>java.lang.String</key-type>" +
        "<value-type>java.lang.String</value-type>" +
        "<resources>" +
        "<heap unit='entries'>10</heap>" +
        "<baz:baz/>" +
        "</resources>" +
        "<foo:foo/>" +
        "</cache>" +
        "</config>" +
        "</configuration>" +
        "</configurations>").ignoreWhitespace().ignoreComments());
  }

  @Test(expected = XmlConfigurationException.class)
  public void testParseOrdinaryConfiguration() {
    XmlMultiConfiguration.from(getClass().getResource("/configs/one-cache.xml")).build();
  }

  private static Configuration emptyConfiguration() {
    return ConfigurationBuilder.newConfigurationBuilder().build();
  }

  private static void assertThrows(Runnable task, Class<? extends Throwable> exception) {
    try {
      task.run();
      fail("Expected " + exception.getSimpleName());
    } catch (AssertionError e) {
      throw e;
    } catch (Throwable t) {
      assertThat(t, instanceOf(exception));
    }
  }
}
