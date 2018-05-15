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

import org.ehcache.clustered.client.config.builders.ClusteredStoreConfigurationBuilder;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.xml.XmlConfiguration;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class ClusteringCacheServiceConfigurationParserTest {

  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");

  @ClassRule
  public static final TemporaryFolder folder = new TemporaryFolder();

  @Rule
  public final TestName testName = new TestName();

  @Test
  public void testTranslateServiceStoreConfiguration() throws Exception {

    ClusteringCacheServiceConfigurationParser configurationTranslator = new ClusteringCacheServiceConfigurationParser();
    Collection<Source> schemaSources = new ArrayList<>();
    schemaSources.add(new StreamSource(CORE_SCHEMA_URL.openStream()));
    Node retNode = configurationTranslator.unparseServiceConfiguration(
      ClusteredStoreConfigurationBuilder.withConsistency(Consistency.STRONG).build());

    assertThat(retNode, is(notNullValue()));
    assertThat(retNode.getNodeName(), is(notNullValue()));
    assertThat(retNode.getNodeName(), is("tc:clustered-store"));
    assertThat(retNode.getAttributes(), is(notNullValue()));
    assertThat(retNode.getAttributes().getLength(), is(2));
    NamedNodeMap nameNodeMap = retNode.getAttributes();
    boolean foundConsistencyNode = false;
    for (int i = 0; i < nameNodeMap.getLength(); i++) {
      Node attributre = nameNodeMap.item(i);
      String attributeName = attributre.getNodeName();
      if ("consistency".equals(attributeName)) {
        String attributeValue = attributre.getNodeValue();
        assertThat(attributeValue, is(notNullValue()));
        assertThat(attributeValue, is(Consistency.STRONG.name().toLowerCase()));
        foundConsistencyNode = true;
        break;
      }
    }
    if (!foundConsistencyNode) {
      fail("Attribute consistency is not found");
    }
  }
}
