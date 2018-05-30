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
package org.ehcache.transactions.xa.internal.xml;

import org.ehcache.transactions.xa.configuration.XAStoreConfiguration;
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
 * TxCacheServiceConfigurationParserTest
 */
public class TxCacheServiceConfigurationParserTest {

  @Test
  public void testTranslateServiceConfiguration() {
    TxCacheServiceConfigurationParser configTranslator = new TxCacheServiceConfigurationParser();
    XAStoreConfiguration storeConfiguration = new XAStoreConfiguration("my-unique-resource");

    Node retElement = configTranslator.unparseServiceConfiguration(storeConfiguration);
    assertThat(retElement, is(notNullValue()));
    assertAttributeItems(retElement);
    assertThat(retElement.getNodeName(), is("tx:xa-store"));
    assertThat(retElement.getFirstChild(), is(nullValue()));
  }

  private void assertAttributeItems(Node retElement) {
    NamedNodeMap node = retElement.getAttributes();
    assertThat(node, is(notNullValue()));
    assertThat(node.getLength(), is(1));
    assertItemNameAndValue(node, 0, "unique-XAResource-id", "my-unique-resource");
  }

  private void assertItemNameAndValue(NamedNodeMap node, int index, String name, String value) {
    assertThat(node.item(index).getNodeName(), is(name));
    assertThat(node.item(index).getNodeValue(), is(value));
  }

}
