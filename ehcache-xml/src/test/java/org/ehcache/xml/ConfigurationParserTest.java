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
package org.ehcache.xml;

import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import java.io.IOException;
import java.io.InputStream;

import static org.ehcache.xml.XmlConfiguration.CORE_SCHEMA_URL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;

public class ConfigurationParserTest {

  @Test
  public void testClasspathContainsValidDiscoverableSchema() throws SAXException, IOException {
    try (InputStream inputStream = CORE_SCHEMA_URL.openStream()) {
      Schema schema = ConfigurationParser.discoverSchema(new StreamSource(inputStream));
      assertThat(schema, notNullValue());
    }
  }
}
