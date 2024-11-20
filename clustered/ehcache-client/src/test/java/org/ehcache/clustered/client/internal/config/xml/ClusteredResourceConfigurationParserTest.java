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

import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.clustered.client.internal.config.ClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.StringReader;

import static org.ehcache.xml.XmlConfigurationMatchers.isSameConfigurationAs;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/**
 * ClusteredResourceConfigurationParserTest
 */
public class ClusteredResourceConfigurationParserTest {

  @Test
  public void testClusteredSharedUsingProperties() throws ParserConfigurationException, IOException, SAXException {
    String property = ClusteredResourceConfigurationParserTest.class.getName() + ":sharing";
    String inputString = "<tc:clustered-shared xmlns:tc='http://www.ehcache.org/v3/clustered' sharing='${" + property + "}'/>";

    ClusteredResourceConfigurationParser parser = new ClusteredResourceConfigurationParser();

    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    Element node =  documentBuilderFactory.newDocumentBuilder()
      .parse(new InputSource(new StringReader(inputString))).getDocumentElement();

    System.setProperty(property, "foobar");
    try {
      SharedClusteredResourcePool configuration = (SharedClusteredResourcePool) parser.parseResourceConfig(node);

      assertThat(configuration.getSharedResourcePool(), is("foobar"));
    } finally {
      System.clearProperty(property);
    }
  }

  @Test
  public void testClusteredDedicatedUsingProperties() throws ParserConfigurationException, IOException, SAXException {
    String fromProperty = ClusteredResourceConfigurationParserTest.class.getName() + ":from";
    String sizeProperty = ClusteredResourceConfigurationParserTest.class.getName() + ":size";
    String inputString = "<tc:clustered-dedicated xmlns:tc='http://www.ehcache.org/v3/clustered' from='${" + fromProperty + "}' unit='B'>" +
      "${" + sizeProperty + "}</tc:clustered-dedicated>";

    ClusteredResourceConfigurationParser parser = new ClusteredResourceConfigurationParser();

    DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
    documentBuilderFactory.setNamespaceAware(true);
    Element node =  documentBuilderFactory.newDocumentBuilder()
      .parse(new InputSource(new StringReader(inputString))).getDocumentElement();

    System.setProperty(fromProperty, "foobar");
    System.setProperty(sizeProperty, "1024");
    try {
      DedicatedClusteredResourcePool configuration = (DedicatedClusteredResourcePool) parser.parseResourceConfig(node);

      assertThat(configuration.getFromResource(), is("foobar"));
      assertThat(configuration.getSize(), is(1024L));
    } finally {
      System.clearProperty(fromProperty);
      System.clearProperty(sizeProperty);
    }
  }

  @Test
  public void testTranslateClusteredResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    ClusteredResourcePoolImpl clusteredResourcePool = new ClusteredResourcePoolImpl();
    Node retElement = configTranslator.unparseResourcePool(clusteredResourcePool);
    String inputString = "<tc:clustered xmlns:tc = \"http://www.ehcache.org/v3/clustered\" />";
    assertThat(retElement, isSameConfigurationAs(inputString));
  }

  @Test
  public void testTranslateDedicatedResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    DedicatedClusteredResourcePoolImpl dedicatedClusteredResourcePool = new DedicatedClusteredResourcePoolImpl("my-from", 12, MemoryUnit.GB);
    Node retElement = configTranslator.unparseResourcePool(dedicatedClusteredResourcePool);
    String inputString = "<tc:clustered-dedicated from = \"my-from\" unit = \"GB\" " +
                         "xmlns:tc = \"http://www.ehcache.org/v3/clustered\">12</tc:clustered-dedicated>";
    assertThat(retElement, isSameConfigurationAs(inputString));
  }

  @Test
  public void testTranslateSharedResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    SharedClusteredResourcePoolImpl sharedResourcePool = new SharedClusteredResourcePoolImpl("shared-pool");
    Node retElement = configTranslator.unparseResourcePool(sharedResourcePool);
    String inputString = "<tc:clustered-shared sharing = \"shared-pool\" " +
                         "xmlns:tc = \"http://www.ehcache.org/v3/clustered\"></tc:clustered-shared>";
    assertThat(retElement, isSameConfigurationAs(inputString));
  }

}
