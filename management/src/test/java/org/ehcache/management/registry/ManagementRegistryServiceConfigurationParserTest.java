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
package org.ehcache.management.registry;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * ManagementRegistryServiceConfigurationParserTest
 */
public class ManagementRegistryServiceConfigurationParserTest {

  @ClassRule
  public static final TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();

  @Test
  public void testTranslateServiceCreationConfiguration() {
    ManagementRegistryServiceConfigurationParser configTranslator = new ManagementRegistryServiceConfigurationParser();

    DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration =
      new DefaultManagementRegistryConfiguration().setCacheManagerAlias("my-cache-alias").
        setCollectorExecutorAlias("my-executor").addTag("tag1").addTag("tag2");

    Node retElement = configTranslator.unparseServiceCreationConfiguration(defaultManagementRegistryConfiguration);
    assertThat(retElement, is(notNullValue()));
    assertThat(retElement.getNodeName(), is("mgm:management"));
    checkAttributeItems(retElement);

    checkTags(retElement);

  }

  @Test
  public void testTranslateServiceCreationConfigurationWithoutTags() {
    ManagementRegistryServiceConfigurationParser configTranslator = new ManagementRegistryServiceConfigurationParser();

    DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration =
      new DefaultManagementRegistryConfiguration().setCacheManagerAlias("my-cache-alias").
        setCollectorExecutorAlias("my-executor");

    Node retElement = configTranslator.unparseServiceCreationConfiguration(defaultManagementRegistryConfiguration);
    assertThat(retElement, is(notNullValue()));
    assertThat(retElement.getNodeName(), is("mgm:management"));
    checkAttributeItems(retElement);
    assertThat(retElement.getFirstChild(), is(nullValue()));
  }

  private void checkAttributeItems(Node element) {
    if (element.getNodeName().equals("mgm:management")) {
      NamedNodeMap node = element.getAttributes();
      assertThat(node, is(notNullValue()));
      assertThat(node.getLength(), is(3));
      assertThat(node.item(0).getNodeName(), is("cache-manager-alias"));
      assertThat(node.item(0).getNodeValue(), is("my-cache-alias"));
      assertThat(node.item(1).getNodeName(), is("collector-executor-alias"));
      assertThat(node.item(1).getNodeValue(), is("my-executor"));
      assertThat(node.item(2).getNodeName(), is("xmlns:mgm"));
      assertThat(node.item(2).getNodeValue(), is("http://www.ehcache.org/v3/management"));
    }

  }

  private void checkTags(Node element) {
    Node tagsElement = element.getFirstChild();
    if (tagsElement != null) {
      assertThat(tagsElement.getNodeName(), is("mgm:tags"));
      assertThat(tagsElement.getAttributes().getLength(), is(0));
      Node tag1 = tagsElement.getFirstChild();
      assertThat(tag1, is(notNullValue()));
      assertThat(tag1.getFirstChild(), is(notNullValue()));
      assertThat(tag1.getFirstChild().getNodeValue(), is("tag1"));
      Node tag2 = tagsElement.getLastChild();
      assertThat(tag2, is(notNullValue()));
      assertThat(tag2.getFirstChild(), is(notNullValue()));
      assertThat(tag2.getFirstChild().getNodeValue(), is("tag2"));
    }

  }


}
