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

import org.ehcache.clustered.client.internal.config.ClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.config.units.MemoryUnit;
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
 * ClusteredResourceConfigurationParserTest
 */
public class ClusteredResourceConfigurationParserTest {
  @ClassRule
  public static final TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();

  @Test
  public void testTranslateClusteredResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    ClusteredResourcePoolImpl clusteredResourcePool = new ClusteredResourcePoolImpl();
    Node retElement = configTranslator.unparseResourcePool(clusteredResourcePool);
    assertThat(retElement, is(notNullValue()));
    assertThat(retElement.getNodeName(), is("ehcache:resources"));
    checkAttributeItems(retElement);

    Node clusterElement = retElement.getFirstChild();
    assertThat(clusterElement, is(notNullValue()));
    assertThat(clusterElement.getNodeName(), is("tc:clustered"));
    checkAttributeItems(clusterElement);
    assertThat(clusterElement.getFirstChild(), is(nullValue()));
  }

  @Test
  public void testTranslateDedicatedResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    DedicatedClusteredResourcePoolImpl dedicatedClusteredResourcePool = new DedicatedClusteredResourcePoolImpl("from", 12, MemoryUnit.GB);
    Node retElement = configTranslator.unparseResourcePool(dedicatedClusteredResourcePool);
    assertThat(retElement, is(notNullValue()));
    assertThat(retElement.getNodeName(), is("ehcache:resources"));
    checkAttributeItems(retElement);

    Node clusterElement = retElement.getFirstChild();
    assertThat(clusterElement, is(notNullValue()));
    assertThat(clusterElement.getNodeName(), is("tc:clustered-dedicated"));
    checkAttributeItems(clusterElement);
    assertThat(clusterElement.getFirstChild().getNodeValue(), is("12"));
  }

  @Test
  public void testTranslateSharedResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    SharedClusteredResourcePoolImpl sharedResourcePool = new SharedClusteredResourcePoolImpl("shared-pool");
    Node retElement = configTranslator.unparseResourcePool(sharedResourcePool);
    assertThat(retElement, is(notNullValue()));
    assertThat(retElement.getNodeName(), is("ehcache:resources"));
    checkAttributeItems(retElement);

    Node clusterElement = retElement.getFirstChild();
    assertThat(clusterElement, is(notNullValue()));
    assertThat(clusterElement.getNodeName(), is("tc:clustered-shared"));
    checkAttributeItems(clusterElement);
    assertThat(clusterElement.getFirstChild(), is(nullValue()));
  }

  public void checkAttributeItems(Node element) {
    if (element.getNodeName().equals("ehcache:resources")) {
      NamedNodeMap node = element.getAttributes();
      assertThat(node, is(notNullValue()));
      assertThat(node.getLength(), is(1));
      assertThat(node.item(0).getNodeName(), is("xmlns:ehcache"));
      assertThat(node.item(0).getNodeValue(), is("http://www.ehcache.org/v3"));
    } else if (element.getNodeName().equals("tc:clustered-shared")) {
      NamedNodeMap node = element.getAttributes();
      assertThat(node, is(notNullValue()));
      assertThat(node.getLength(), is(2));
      assertThat(node.item(0).getNodeName(), is("sharing"));
      assertThat(node.item(0).getNodeValue(), is("shared-pool"));
      assertThat(node.item(1).getNodeName(), is("xmlns:tc"));
      assertThat(node.item(1).getNodeValue(), is("http://www.ehcache.org/v3/clustered"));
    } else if (element.getNodeName().equals("tc:clustered-dedicated")) {
      NamedNodeMap node = element.getAttributes();
      assertThat(node, is(notNullValue()));
      assertThat(node.getLength(), is(3));
      assertThat(node.item(0).getNodeName(), is("from"));
      assertThat(node.item(0).getNodeValue(), is("from"));
      assertThat(node.item(1).getNodeName(), is("unit"));
      assertThat(node.item(1).getNodeValue(), is("GB"));
      assertThat(node.item(2).getNodeName(), is("xmlns:tc"));
      assertThat(node.item(2).getNodeValue(), is("http://www.ehcache.org/v3/clustered"));
    }
  }
}
