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
package org.ehcache.jsr107;

import org.ehcache.config.Configuration;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.junit.Test;

import java.net.URL;

/**
 * Jsr107CacheParserIT
 */
public class Jsr107CacheParserIT {

  @Test(expected = XmlConfigurationException.class)
  public void testJsr107CacheXmlTranslationToString() {
    URL resource = Jsr107CacheParserIT.class.getResource("/ehcache-107.xml");
    Configuration config = new XmlConfiguration(resource);
    XmlConfiguration xmlConfig = new XmlConfiguration(config);
  }
}
