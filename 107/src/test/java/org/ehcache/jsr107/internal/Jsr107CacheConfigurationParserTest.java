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

import org.ehcache.jsr107.config.ConfigurationElementState;
import org.ehcache.jsr107.config.Jsr107CacheConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.junit.Test;

/**
 * Jsr107CacheConfigurationParserTest
 */
public class Jsr107CacheConfigurationParserTest {

  @Test(expected = XmlConfigurationException.class)
  public void testTranslateServiceCreationConfigurationWithStatisticsManagementEnabled() {
    Jsr107CacheConfigurationParser configTranslator = new Jsr107CacheConfigurationParser();
    Jsr107CacheConfiguration cacheConfiguration =
      new Jsr107CacheConfiguration(ConfigurationElementState.ENABLED, ConfigurationElementState.DISABLED);
    configTranslator.unparseServiceConfiguration(cacheConfiguration);
  }

}
