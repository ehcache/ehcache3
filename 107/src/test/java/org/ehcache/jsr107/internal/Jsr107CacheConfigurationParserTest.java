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
package org.ehcache.jsr107.internal;

import org.ehcache.jsr107.config.ConfigurationElementState;
import org.ehcache.jsr107.config.Jsr107CacheConfiguration;
import org.junit.Test;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Jsr107CacheConfigurationParserTest
 */
public class Jsr107CacheConfigurationParserTest {

  @Test
  public void testTranslateServiceCreationConfigurationWithStatisticsManagementEnabled() {
    Jsr107CacheConfigurationParser configTranslator = new Jsr107CacheConfigurationParser();
    Jsr107CacheConfiguration cacheConfiguration =
      new Jsr107CacheConfiguration(ConfigurationElementState.ENABLED, ConfigurationElementState.DISABLED);
    Node retElement = configTranslator.unparseServiceConfiguration(cacheConfiguration);
    assertThat(retElement, is(notNullValue()));
    assertAttributeItemsWithStatisticsManagementEnabled(retElement);
    assertThat(retElement.getNodeName(), is("jsr107:mbeans"));
    assertThat(retElement.getFirstChild(), is(nullValue()));
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithStatisticsManagementUnspecified() {
    Jsr107CacheConfigurationParser configTranslator = new Jsr107CacheConfigurationParser();
    Jsr107CacheConfiguration cacheConfiguration =
      new Jsr107CacheConfiguration(ConfigurationElementState.UNSPECIFIED, ConfigurationElementState.UNSPECIFIED);
    Node retElement = configTranslator.unparseServiceConfiguration(cacheConfiguration);
    assertThat(retElement, is(notNullValue()));
    assertAttributeItemsWithStatisticsManagementUnspecified(retElement);
    assertThat(retElement.getNodeName(), is("jsr107:mbeans"));
    assertThat(retElement.getFirstChild(), is(nullValue()));
  }


  private void assertAttributeItemsWithStatisticsManagementUnspecified(Node retElement) {
    NamedNodeMap node = retElement.getAttributes();
    assertThat(node, is(notNullValue()));
    assertThat(node.getLength(), is(0));
  }

  private void assertItemNameAndValue(NamedNodeMap node, int index, String name, String value) {
    assertThat(node.item(index).getNodeName(), is(name));
    assertThat(node.item(index).getNodeValue(), is(value));
  }

  private void assertAttributeItemsWithStatisticsManagementEnabled(Node retElement) {
    NamedNodeMap node = retElement.getAttributes();
    assertThat(node, is(notNullValue()));
    assertThat(node.getLength(), is(2));
    assertItemNameAndValue(node, 0, "enable-management", "false");
    assertItemNameAndValue(node, 1, "enable-statistics", "true");
  }
}
