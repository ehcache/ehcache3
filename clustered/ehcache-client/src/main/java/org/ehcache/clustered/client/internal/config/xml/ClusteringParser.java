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

import org.ehcache.xml.BaseConfigParser;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static java.util.stream.Stream.concat;

public abstract class ClusteringParser<T> extends BaseConfigParser<T> {

  protected static final String NAMESPACE = "http://www.ehcache.org/v3/clustered";
  protected static final String TC_CLUSTERED_NAMESPACE_PREFIX = "tc:";

  public ClusteringParser() {
    this(emptyMap());
  }

  public ClusteringParser(Map<URI, URL> namespaces) {
    super(concat(singletonMap(URI.create(NAMESPACE), ClusteringParser.class.getResource("/ehcache-clustered-ext.xsd")).entrySet().stream(),
      namespaces.entrySet().stream()
    ).collect(toLinkedHashMap(Map.Entry::getKey, Map.Entry::getValue)));
  }

  protected static Optional<Element> childElementOf(Element fragment, Predicate<Element> predicate) {
    Collection<Element> elements = new ArrayList<>();
    NodeList children = fragment.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);
      if (child instanceof Element && predicate.test((Element) child)) {
        elements.add((Element) child);
      }
    }
    switch (elements.size()) {
      case 0:
        return Optional.empty();
      case 1:
        return Optional.of(elements.iterator().next());
      default:
        throw new AssertionError("Validation Leak! {" + elements + "}");
    }
  }

  protected static <T extends Node> Optional<T> optionalSingleton(Class<T> nodeKlass, NodeList nodes) {
    switch (nodes.getLength()) {
      case 0:
        return Optional.empty();
      case 1:
        return Optional.of(nodeKlass.cast(nodes.item(0)));
      default:
        throw new AssertionError("Validation Leak! {" + nodes + "}");
    }
  }
}
