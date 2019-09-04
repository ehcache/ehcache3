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
import org.ehcache.jsr107.config.Jsr107Configuration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Jsr107ServiceConfigurationParserTest
 */
public class Jsr107ServiceConfigurationParserTest {

  @Test(expected = XmlConfigurationException.class)
  public void testTranslateServiceCreationConfiguration() {
    Jsr107ServiceConfigurationParser configTranslator = new Jsr107ServiceConfigurationParser();

    Map<String, String> templateMap = new HashMap<>();
    templateMap.put("testCache", "simpleCacheTemplate");
    templateMap.put("testCache1", "simpleCacheTemplate1");
    boolean jsr107CompliantAtomics = true;
    Jsr107Configuration serviceCreationConfiguration =
      new Jsr107Configuration("tiny-template", templateMap, jsr107CompliantAtomics,
        ConfigurationElementState.ENABLED, ConfigurationElementState.DISABLED);

    configTranslator.unparseServiceCreationConfiguration(serviceCreationConfiguration);
  }

}
