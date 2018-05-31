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
import org.junit.Test;
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

  @Test
  public void testTranslateClusteredResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    ClusteredResourcePoolImpl clusteredResourcePool = new ClusteredResourcePoolImpl();
    Node retElement = configTranslator.unparseResourcePool(clusteredResourcePool);
    assertThat(retElement, is(notNullValue()));
    assertThat(retElement.getNodeName(), is("tc:clustered"));
    assertAttributeItems(retElement);
    assertThat(retElement.getFirstChild(), is(nullValue()));
  }

  @Test
  public void testTranslateDedicatedResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    DedicatedClusteredResourcePoolImpl dedicatedClusteredResourcePool = new DedicatedClusteredResourcePoolImpl("from", 12, MemoryUnit.GB);
    Node retElement = configTranslator.unparseResourcePool(dedicatedClusteredResourcePool);
    assertThat(retElement, is(notNullValue()));
    assertThat(retElement.getNodeName(), is("tc:clustered-dedicated"));
    assertAttributeItems(retElement);
    assertThat(retElement.getFirstChild().getNodeValue(), is("12"));
  }

  @Test
  public void testTranslateSharedResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    SharedClusteredResourcePoolImpl sharedResourcePool = new SharedClusteredResourcePoolImpl("shared-pool");
    Node retElement = configTranslator.unparseResourcePool(sharedResourcePool);
    assertThat(retElement, is(notNullValue()));
    assertThat(retElement.getNodeName(), is("tc:clustered-shared"));
    assertAttributeItems(retElement);
    assertThat(retElement.getFirstChild(), is(nullValue()));
  }

  private void assertAttributeItems(Node element) {
    if (element.getNodeName().equals("tc:clustered-shared")) {
      NamedNodeMap node = element.getAttributes();
      assertThat(node, is(notNullValue()));
      assertThat(node.getLength(), is(1));
      assertItemNameAndValue(node, 0, "sharing", "shared-pool");
    } else if (element.getNodeName().equals("tc:clustered-dedicated")) {
      NamedNodeMap node = element.getAttributes();
      assertThat(node, is(notNullValue()));
      assertThat(node.getLength(), is(2));
      assertItemNameAndValue(node, 0, "from", "from");
      assertItemNameAndValue(node, 1, "unit", "GB");
    } else if (element.getNodeName().equals("tc:clustered")) {
      NamedNodeMap node = element.getAttributes();
      assertThat(node, is(notNullValue()));
      assertThat(node.getLength(), is(0));
    }
  }

  private void assertItemNameAndValue(NamedNodeMap node, int index, String name, String value) {
    assertThat(node.item(index).getNodeName(), is(name));
    assertThat(node.item(index).getNodeValue(), is(value));
  }

}
