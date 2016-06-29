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

package org.ehcache.xml;

import org.ehcache.config.ResourcePool;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;

import javax.xml.transform.Source;

/**
 * Defines a handler for processing {@code /config/cache/resources} extension elements.
 *
 * @author Clifford W. Johnson
 */
public interface CacheResourceConfigurationParser {

  Source getXmlSchema() throws IOException;

  URI getNamespace();

  ResourcePool parseResourceConfiguration(Element fragment);
}
