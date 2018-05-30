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

import org.ehcache.transactions.xa.txmgr.btm.BitronixTransactionManagerLookup;
import org.ehcache.transactions.xa.txmgr.provider.LookupTransactionManagerProviderConfiguration;
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
 * TxCacheManagerServiceConfigurationParserTest
 */
public class TxCacheManagerServiceConfigurationParserTest {

  @Test
  public void testTranslateServiceCreationConfiguration() {
    TxCacheManagerServiceConfigurationParser configTranslator = new TxCacheManagerServiceConfigurationParser();
    LookupTransactionManagerProviderConfiguration lookupTransactionManagerProviderConfiguration =
      new LookupTransactionManagerProviderConfiguration(BitronixTransactionManagerLookup.class);

    Node retElement = configTranslator.unparseServiceCreationConfiguration(lookupTransactionManagerProviderConfiguration);
    assertThat(retElement, is(notNullValue()));
    assertAttributeItems(retElement);
    assertThat(retElement.getNodeName(), is("tx:jta-tm"));
    assertThat(retElement.getFirstChild(), is(nullValue()));
  }

  private void assertAttributeItems(Node retElement) {
    NamedNodeMap node = retElement.getAttributes();
    assertThat(node, is(notNullValue()));
    assertThat(node.getLength(), is(1));
    assertItemNameAndValue(node, 0, "transaction-manager-lookup-class", "org.ehcache.transactions.xa.txmgr.btm.BitronixTransactionManagerLookup");
  }

  private void assertItemNameAndValue(NamedNodeMap node, int index, String name, String value) {
    assertThat(node.item(index).getNodeName(), is(name));
    assertThat(node.item(index).getNodeValue(), is(value));
  }
}
