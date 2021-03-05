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
import org.w3c.dom.Node;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.ElementSelectors;

import static org.junit.Assert.assertThat;
import static org.xmlunit.matchers.CompareMatcher.isSimilarTo;

/**
 * ClusteredResourceConfigurationParserTest
 */
public class ClusteredResourceConfigurationParserTest {

  @Test
  public void testTranslateClusteredResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    ClusteredResourcePoolImpl clusteredResourcePool = new ClusteredResourcePoolImpl();
    Node retElement = configTranslator.unparseResourcePool(clusteredResourcePool);
    String inputString = "<tc:clustered xmlns:tc = \"http://www.ehcache.org/v3/clustered\" />";
    assertThat(retElement, isSimilarTo(inputString).ignoreComments().ignoreWhitespace()
      .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
  }

  @Test
  public void testTranslateDedicatedResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    DedicatedClusteredResourcePoolImpl dedicatedClusteredResourcePool = new DedicatedClusteredResourcePoolImpl("my-from", 12, MemoryUnit.GB);
    Node retElement = configTranslator.unparseResourcePool(dedicatedClusteredResourcePool);
    String inputString = "<tc:clustered-dedicated from = \"my-from\" unit = \"GB\" " +
                         "xmlns:tc = \"http://www.ehcache.org/v3/clustered\">12</tc:clustered-dedicated>";
    assertThat(retElement, isSimilarTo(inputString).ignoreComments().ignoreWhitespace()
      .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
  }

  @Test
  public void testTranslateSharedResourcePoolConfiguration() {
    ClusteredResourceConfigurationParser configTranslator = new ClusteredResourceConfigurationParser();
    SharedClusteredResourcePoolImpl sharedResourcePool = new SharedClusteredResourcePoolImpl("shared-pool");
    Node retElement = configTranslator.unparseResourcePool(sharedResourcePool);
    String inputString = "<tc:clustered-shared sharing = \"shared-pool\" " +
                         "xmlns:tc = \"http://www.ehcache.org/v3/clustered\"></tc:clustered-shared>";
    assertThat(retElement, isSimilarTo(inputString).ignoreComments().ignoreWhitespace()
      .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
  }

}
