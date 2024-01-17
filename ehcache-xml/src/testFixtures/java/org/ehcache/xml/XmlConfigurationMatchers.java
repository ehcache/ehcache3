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

import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.ElementSelector;
import org.xmlunit.matchers.CompareMatcher;
import org.xmlunit.util.Nodes;

import javax.xml.namespace.QName;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.xmlunit.diff.ElementSelectors.Default;
import static org.xmlunit.diff.ElementSelectors.byName;
import static org.xmlunit.diff.ElementSelectors.byNameAndAttributes;
import static org.xmlunit.diff.ElementSelectors.byNameAndText;
import static org.xmlunit.diff.ElementSelectors.conditionalSelector;
import static org.xmlunit.diff.ElementSelectors.selectorForElementNamed;

public class XmlConfigurationMatchers {

  private static final String EHCACHE_NAMESPACE = "http://www.ehcache.org/v3";
  private static final QName CACHE_QNAME = new QName(EHCACHE_NAMESPACE, "cache");
  private static final QName RESOURCES_QNAME = new QName(EHCACHE_NAMESPACE, "resources");
  private static final QName SHARED_RESOURCES_QNAME = new QName(EHCACHE_NAMESPACE, "shared-resources");
  private static final QName EVENTS_TO_FIRE_ON_QNAME = new QName(EHCACHE_NAMESPACE, "events-to-fire-on");

  private static final String MULTI_NAMESPACE = "http://www.ehcache.org/v3/multi";
  private static final QName MULTI_CONFIGURATION_QNAME = new QName(MULTI_NAMESPACE, "configuration");

  public static CompareMatcher isSameConfigurationAs(Object input, ElementSelector... extraElementSelectors) {
    List<ElementSelector> elementSelectors = new ArrayList<>(asList(extraElementSelectors));
    elementSelectors.add(selectorForElementNamed(MULTI_CONFIGURATION_QNAME, byNameAndAttributes("identity")));
    elementSelectors.add(selectorForElementNamed(EVENTS_TO_FIRE_ON_QNAME, byNameAndText));
    elementSelectors.add(selectorForElementNamed(CACHE_QNAME, byNameAndAttributes("alias")));
    elementSelectors.add(conditionalSelector(element -> Nodes.getQName(element.getParentNode()).equals(RESOURCES_QNAME), byName));
    elementSelectors.add(conditionalSelector(element -> Nodes.getQName(element.getParentNode()).equals(SHARED_RESOURCES_QNAME), byName));
    elementSelectors.add(Default);

    return CompareMatcher.isSimilarTo(input).ignoreComments().ignoreWhitespace()
      .withNodeMatcher(new DefaultNodeMatcher(elementSelectors.toArray(new ElementSelector[0])));
  }
}
