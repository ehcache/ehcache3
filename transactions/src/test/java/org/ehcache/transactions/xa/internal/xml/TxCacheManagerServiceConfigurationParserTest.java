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
import org.junit.Test;
import org.w3c.dom.Node;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.ElementSelectors;


import static org.junit.Assert.assertThat;
import static org.xmlunit.matchers.CompareMatcher.isSimilarTo;

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
    String inputString = "<tx:jta-tm " +
                         "transaction-manager-lookup-class = \"org.ehcache.transactions.xa.txmgr.btm.BitronixTransactionManagerLookup\" " +
                         "xmlns:tx = \"http://www.ehcache.org/v3/tx\" />";
    assertThat(retElement, isSimilarTo(inputString).ignoreComments().ignoreWhitespace()
      .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndAllAttributes)));
  }

}
