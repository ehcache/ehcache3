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
package org.ehcache.jsr107.internal;

import org.ehcache.config.Configuration;
import org.ehcache.jsr107.config.ConfigurationElementState;
import org.ehcache.jsr107.config.Jsr107CacheConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.XmlConfigurationTest;
import org.junit.Test;
import org.w3c.dom.Node;
import org.xmlunit.builder.Input;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.ElementSelectors;

import java.net.URL;

import static org.ehcache.xml.ConfigurationParserTestHelper.assertElement;
import static org.junit.Assert.assertThat;
import static org.xmlunit.matchers.CompareMatcher.isSimilarTo;

/**
 * Jsr107CacheConfigurationParserTest
 */
public class Jsr107CacheConfigurationParserTest {

  @Test
  public void testTranslateServiceCreationConfigurationWithStatisticsManagementEnabled() {
    Jsr107CacheConfigurationParser configTranslator = new Jsr107CacheConfigurationParser();
    Jsr107CacheConfiguration cacheConfiguration =
      new Jsr107CacheConfiguration(ConfigurationElementState.ENABLED, ConfigurationElementState.DISABLED);
    Node retElement = configTranslator.unparseServiceConfiguration(cacheConfiguration);
    String inputString = "<jsr107:mbeans enable-management = \"false\" enable-statistics = \"true\" " +
                         "xmlns:jsr107 = \"http://www.ehcache.org/v3/jsr107\"></jsr107:mbeans>";
    assertElement(inputString, retElement);
  }

  @Test
  public void testTranslateServiceCreationConfigurationWithStatisticsManagementUnspecified() {
    Jsr107CacheConfigurationParser configTranslator = new Jsr107CacheConfigurationParser();
    Jsr107CacheConfiguration cacheConfiguration =
      new Jsr107CacheConfiguration(ConfigurationElementState.UNSPECIFIED, ConfigurationElementState.UNSPECIFIED);
    Node retElement = configTranslator.unparseServiceConfiguration(cacheConfiguration);
    String inputString = "<jsr107:mbeans xmlns:jsr107 = \"http://www.ehcache.org/v3/jsr107\"></jsr107:mbeans>";
    assertElement(inputString, retElement);
  }

}
