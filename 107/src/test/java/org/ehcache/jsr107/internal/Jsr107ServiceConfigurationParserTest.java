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
import org.ehcache.jsr107.config.Jsr107Configuration;
import org.junit.Test;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * Jsr107ServiceConfigurationParserTest
 */
public class Jsr107ServiceConfigurationParserTest {
  @Test
  public void testTranslateServiceCreationConfiguration() {
    Jsr107ServiceConfigurationParser configTranslator = new Jsr107ServiceConfigurationParser();

    Map<String, String> templateMap = new HashMap<>();
    templateMap.put("testCache", "simpleCacheTemplate");
    templateMap.put("testCache1", "simpleCacheTemplate1");
    boolean jsr107CompliantAtomics = true;
    Jsr107Configuration serviceCreationConfiguration =
      new Jsr107Configuration("tiny-template", templateMap, jsr107CompliantAtomics,
        ConfigurationElementState.ENABLED, ConfigurationElementState.DISABLED);

    Node retElement = configTranslator.unparseServiceCreationConfiguration(serviceCreationConfiguration);
    assertThat(retElement, is(notNullValue()));
    assertAttributeItems(retElement);
    assertThat(retElement.getNodeName(), is("jsr107:defaults"));
    assertThat(retElement.getFirstChild(), is(notNullValue()));
    assertCaches(retElement.getFirstChild());
    assertThat(retElement.getLastChild(), is(notNullValue()));
    assertCaches(retElement.getLastChild());
  }

  @Test
  public void testTranslateServiceWithManagementStatisticsUnspecifiedAndNoCaches() {
    Jsr107ServiceConfigurationParser configTranslator = new Jsr107ServiceConfigurationParser();
    boolean jsr107CompliantAtomics = false;
    Map<String, String> templateMap = new HashMap<>();
    Jsr107Configuration serviceCreationConfiguration =
      new Jsr107Configuration("tiny-template", templateMap, jsr107CompliantAtomics,
        ConfigurationElementState.UNSPECIFIED, ConfigurationElementState.UNSPECIFIED);

    Node retElement = configTranslator.unparseServiceCreationConfiguration(serviceCreationConfiguration);
    assertThat(retElement, is(notNullValue()));
    assertAttributeItemsWithManagementStatisticsUnspecifiedAndNoCaches(retElement);
    assertThat(retElement.getNodeName(), is("jsr107:defaults"));
    assertThat(retElement.getFirstChild(), is(nullValue()));
  }

  private void assertAttributeItems(Node retElement) {
    NamedNodeMap node = retElement.getAttributes();
    assertThat(node, is(notNullValue()));
    assertThat(node.getLength(), is(4));
    assertItemNameAndValue(node, 0, "default-template", "tiny-template");
    assertItemNameAndValue(node, 1, "enable-management", "true");
    assertItemNameAndValue(node, 2, "enable-statistics", "false");
    assertItemNameAndValue(node, 3, "jsr-107-compliant-atomics", "true");
  }

  private void assertItemNameAndValue(NamedNodeMap node, int index, String name, String value) {
    assertThat(node.item(index).getNodeName(), is(name));
    assertThat(node.item(index).getNodeValue(), is(value));
  }

  private void assertAttributeItemsWithManagementStatisticsUnspecifiedAndNoCaches(Node retElement) {
    NamedNodeMap node = retElement.getAttributes();
    assertThat(node, is(notNullValue()));
    assertThat(node.getLength(), is(2));
    assertItemNameAndValue(node, 0, "default-template", "tiny-template");
    assertItemNameAndValue(node, 1, "jsr-107-compliant-atomics", "false");
  }

  private void assertCaches(Node retElement) {
    assertThat(retElement.getNodeName(), is("jsr107:cache"));
    NamedNodeMap node = retElement.getAttributes();
    assertThat(node, is(notNullValue()));
    assertThat(node.getLength(), is(2));
    assertThat(node.item(0).getNodeName(), is("name"));
    assertThat(node.item(1).getNodeName(), is("template"));
    if (node.item(0).getNodeValue().equals("testCache")) {
      assertThat(node.item(1).getNodeValue(), is("simpleCacheTemplate"));
    } else if (node.item(0).getNodeValue().equals("testCache1")) {
      assertThat(node.item(1).getNodeValue(), is("simpleCacheTemplate1"));
    }
  }

}
