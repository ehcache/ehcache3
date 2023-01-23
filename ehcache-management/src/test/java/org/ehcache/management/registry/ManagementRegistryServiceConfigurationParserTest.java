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

import org.hamcrest.Matchers;
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

/**
 * ManagementRegistryServiceConfigurationParserTest
 */
public class ManagementRegistryServiceConfigurationParserTest {

  @Test
  public void testParseTagInsideProperty() throws ParserConfigurationException, IOException, SAXException {
    String property = ManagementRegistryServiceConfigurationParserTest.class.getName() + ":tag";
    String inputString = "<mgm:management cache-manager-alias='my-cache-alias' collector-executor-alias='my-executor' " +
      "xmlns:mgm='http://www.ehcache.org/v3/management'>" +
      "<mgm:tags><mgm:tag>tag1</mgm:tag><mgm:tag>${" + property + "}</mgm:tag></mgm:tags></mgm:management>";

    ManagementRegistryServiceConfigurationParser configParser = new ManagementRegistryServiceConfigurationParser();

    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    Element node =  documentBuilderFactory.newDocumentBuilder()
      .parse(new InputSource(new StringReader(inputString))).getDocumentElement();

    System.setProperty(property, "tag2");
    try {
      DefaultManagementRegistryConfiguration configuration = configParser.parse(node, null);

      assertThat(configuration.getTags(), Matchers.hasItems("tag1", "tag2"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testTranslateServiceCreationConfiguration() throws IOException, ParserConfigurationException, SAXException {
    ManagementRegistryServiceConfigurationParser configTranslator = new ManagementRegistryServiceConfigurationParser();

    DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration =
      new DefaultManagementRegistryConfiguration().setCacheManagerAlias("my-cache-alias").
        setCollectorExecutorAlias("my-executor").addTag("tag1").addTag("tag2");

    Node retElement = configTranslator.unparse(createDocumentRoot(configTranslator.getSchema().values()), defaultManagementRegistryConfiguration);
    String inputString = "<mgm:management cache-manager-alias = \"my-cache-alias\" collector-executor-alias = \"my-executor\" " +
      "xmlns:mgm = \"http://www.ehcache.org/v3/management\" >" +
      "<mgm:tags><mgm:tag>tag1</mgm:tag><mgm:tag>tag2</mgm:tag></mgm:tags></mgm:management>";
    assertThat(retElement, isSameConfigurationAs(inputString));
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithoutTags() throws IOException, ParserConfigurationException, SAXException {
    ManagementRegistryServiceConfigurationParser configTranslator = new ManagementRegistryServiceConfigurationParser();

    DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration =
      new DefaultManagementRegistryConfiguration().setCacheManagerAlias("my-cache-alias").
        setCollectorExecutorAlias("my-executor");

    Node retElement = configTranslator.unparse(createDocumentRoot(configTranslator.getSchema().values()), defaultManagementRegistryConfiguration);
    String inputString = "<mgm:management cache-manager-alias = \"my-cache-alias\" collector-executor-alias = \"my-executor\" " +
      "xmlns:mgm = \"http://www.ehcache.org/v3/management\"></mgm:management>";
    assertThat(retElement, isSameConfigurationAs(inputString));
  }

}
