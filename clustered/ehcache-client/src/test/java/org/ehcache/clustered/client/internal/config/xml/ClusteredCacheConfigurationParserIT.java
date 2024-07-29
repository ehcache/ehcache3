/*
 * Copyright Terracotta, Inc.
 * Copyright Super iPaaS Integration LLC, an IBM Company 2024
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

import org.ehcache.config.Configuration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;

import java.net.URL;

import static org.ehcache.xml.XmlConfigurationMatchers.isSameConfigurationAs;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * ClusteredCacheConfigurationParserIT
 */
public class ClusteredCacheConfigurationParserIT {

  @Test
  public void testClusteredCacheXmlTranslationToString() {
    URL resource = ClusteredCacheConfigurationParserIT.class.getResource("/configs/clustered-cache.xml");
    Configuration config = new XmlConfiguration(resource);
    XmlConfiguration xmlConfig = new XmlConfiguration(config);
    assertThat(xmlConfig.toString(), isSameConfigurationAs(resource));
  }
}
