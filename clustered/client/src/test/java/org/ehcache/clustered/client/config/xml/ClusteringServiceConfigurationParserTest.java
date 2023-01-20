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

package org.ehcache.clustered.client.config.xml;

import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.xml.CacheManagerServiceConfigurationParser;
import org.junit.Test;
import org.w3c.dom.Attr;
import org.w3c.dom.Element;

import java.util.ServiceLoader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.stream.StreamSource;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.*;

/**
 * @author Clifford W. Johnson
 */
public class ClusteringServiceConfigurationParserTest {

  /**
   * Ensures the {@link ClusteringServiceConfigurationParser} is locatable as a
   * {@link CacheManagerServiceConfigurationParser} instance.
   */
  @Test
  public void testServiceLocator() throws Exception {
    final String expectedParser = ClusteringServiceConfigurationParser.class.getName();
    final ServiceLoader<CacheManagerServiceConfigurationParser> parsers =
        ClassLoading.libraryServiceLoaderFor(CacheManagerServiceConfigurationParser.class);
    foundParser: {
      for (final CacheManagerServiceConfigurationParser parser : parsers) {
        if (parser.getClass().getName().equals(expectedParser)) {
          break foundParser;
        }
      }
      fail("Expected parser not found");
    }
  }

  /**
   * Ensures the namespace declared by {@link ClusteringServiceConfigurationParser} and its
   * schema are the same.
   */
  @Test
  public void testSchema() throws Exception {
    final ClusteringServiceConfigurationParser parser = new ClusteringServiceConfigurationParser();
    final StreamSource schemaSource = (StreamSource) parser.getXmlSchema();

    final DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);

    final DocumentBuilder domBuilder = factory.newDocumentBuilder();
    final Element schema = domBuilder.parse(schemaSource.getInputStream()).getDocumentElement();
    final Attr targetNamespaceAttr = schema.getAttributeNode("targetNamespace");
    assertThat(targetNamespaceAttr, is(not(nullValue())));
    assertThat(targetNamespaceAttr.getValue(), is(parser.getNamespace().toString()));
  }
}