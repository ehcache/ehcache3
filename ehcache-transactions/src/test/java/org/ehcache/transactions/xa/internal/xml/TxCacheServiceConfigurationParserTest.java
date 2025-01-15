/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

import static org.ehcache.xml.DomUtil.createDocumentRoot;
import static org.ehcache.xml.XmlConfigurationMatchers.isSameConfigurationAs;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * TxCacheServiceConfigurationParserTest
 */
public class TxCacheServiceConfigurationParserTest {

  @Test
  public void testParseXaResourceIdInsideProperty() throws ParserConfigurationException, IOException, SAXException {
    String property = TxCacheManagerServiceConfigurationParserTest.class.getName() + ":xaResourceId";
    String inputString = "<tx:xa-store xmlns:tx='http://www.ehcache.org/v3/tx' unique-XAResource-id='${" + property + "}'/>";

    TxCacheServiceConfigurationParser configParser = new TxCacheServiceConfigurationParser();

    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    Element node =  documentBuilderFactory.newDocumentBuilder()
      .parse(new InputSource(new StringReader(inputString))).getDocumentElement();

    System.setProperty(property, "Brian");
    try {
      XAStoreConfiguration configuration = configParser.parse(node, null);

      assertThat(configuration.getUniqueXAResourceId(), is("Brian"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testTranslateServiceConfiguration() throws IOException, ParserConfigurationException, SAXException {
    TxCacheServiceConfigurationParser configTranslator = new TxCacheServiceConfigurationParser();
    XAStoreConfiguration storeConfiguration = new XAStoreConfiguration("my-unique-resource");

    Node retElement = configTranslator.unparse(createDocumentRoot(configTranslator.getSchema().values()), storeConfiguration);
    String inputString = "<tx:xa-store unique-XAResource-id = \"my-unique-resource\" " +
      "xmlns:tx = \"http://www.ehcache.org/v3/tx\"/>";
    assertThat(retElement, isSameConfigurationAs(inputString));
  }

}
