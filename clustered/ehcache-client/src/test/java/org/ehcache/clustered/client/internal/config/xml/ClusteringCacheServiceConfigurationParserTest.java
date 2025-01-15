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
package org.ehcache.clustered.client.internal.config.xml;

import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.junit.Test;
import org.w3c.dom.Node;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

import static org.ehcache.xml.DomUtil.createDocumentRoot;
import static org.ehcache.xml.XmlConfigurationMatchers.isSameConfigurationAs;
import static org.hamcrest.MatcherAssert.assertThat;

public class ClusteringCacheServiceConfigurationParserTest {

  @Test
  public void testTranslateServiceStoreConfiguration() throws IOException, ParserConfigurationException, SAXException {

    ClusteringCacheServiceConfigurationParser configurationTranslator = new ClusteringCacheServiceConfigurationParser();
    Node retNode = configurationTranslator.unparse(createDocumentRoot(configurationTranslator.getSchema().values()),
      ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG).build());

    String inputString = "<tc:clustered-store consistency = \"strong\" " +
      "xmlns:tc = \"http://www.ehcache.org/v3/clustered\"></tc:clustered-store>";
    assertThat(retNode, isSameConfigurationAs(inputString));
  }
}
