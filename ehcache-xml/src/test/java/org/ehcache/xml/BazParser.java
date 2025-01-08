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

import org.ehcache.config.ResourcePool;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.net.URI;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

/**
 * Parser for the test resource {@code BazResource}
 */
public class BazParser implements CacheResourceConfigurationParser {

  private static final String NAMESPACE = "http://www.example.com/baz";

  @Override
  public Map<URI, Supplier<Source>> getSchema() {
    return Collections.singletonMap(URI.create(NAMESPACE), () -> new StreamSource(getClass().getResourceAsStream("/configs/baz.xsd")));
  }

  @Override
  public ResourcePool parse(Element fragment, ClassLoader classLoader) {
    String elementName = fragment.getLocalName();
    if (elementName.equals("baz")) {
      return new BazResource();
    }
    return null;
  }

  @Override
  public Element unparse(Document target, ResourcePool resourcePool) {
    return target.createElementNS(NAMESPACE, "baz:baz");
  }

  @Override
  public Set<Class<? extends ResourcePool>> getResourceTypes() {
    return Collections.singleton(BazResource.class);
  }
}
