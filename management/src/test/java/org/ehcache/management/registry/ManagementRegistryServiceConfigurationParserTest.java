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

import org.junit.Test;
import org.w3c.dom.Node;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.ElementSelectors;

import static org.junit.Assert.assertThat;
import static org.xmlunit.matchers.CompareMatcher.isSimilarTo;

/**
 * ManagementRegistryServiceConfigurationParserTest
 */
public class ManagementRegistryServiceConfigurationParserTest {

  @Test
  public void testTranslateServiceCreationConfiguration() {
    ManagementRegistryServiceConfigurationParser configTranslator = new ManagementRegistryServiceConfigurationParser();

    DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration =
      new DefaultManagementRegistryConfiguration().setCacheManagerAlias("my-cache-alias").
        setCollectorExecutorAlias("my-executor").addTag("tag1").addTag("tag2");

    Node retElement = configTranslator.unparseServiceCreationConfiguration(defaultManagementRegistryConfiguration);
    String inputString = "<mgm:management cache-manager-alias = \"my-cache-alias\" collector-executor-alias = \"my-executor\" " +
                         "xmlns:mgm = \"http://www.ehcache.org/v3/management\" >" +
                         "<mgm:tags><mgm:tag>tag1</mgm:tag><mgm:tag>tag2</mgm:tag></mgm:tags></mgm:management>";
    assertThat(retElement, isSimilarTo(inputString).ignoreComments().ignoreWhitespace()
      .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithoutTags() {
    ManagementRegistryServiceConfigurationParser configTranslator = new ManagementRegistryServiceConfigurationParser();

    DefaultManagementRegistryConfiguration defaultManagementRegistryConfiguration =
      new DefaultManagementRegistryConfiguration().setCacheManagerAlias("my-cache-alias").
        setCollectorExecutorAlias("my-executor");

    Node retElement = configTranslator.unparseServiceCreationConfiguration(defaultManagementRegistryConfiguration);
    String inputString = "<mgm:management cache-manager-alias = \"my-cache-alias\" collector-executor-alias = \"my-executor\" " +
                         "xmlns:mgm = \"http://www.ehcache.org/v3/management\"></mgm:management>";
    assertThat(retElement, isSimilarTo(inputString).ignoreComments().ignoreWhitespace()
      .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndText)));
  }

}
