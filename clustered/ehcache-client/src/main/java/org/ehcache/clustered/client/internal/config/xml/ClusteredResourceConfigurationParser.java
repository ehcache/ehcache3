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

package org.ehcache.clustered.client.internal.config.xml;

import org.ehcache.clustered.client.config.ClusteredResourcePool;
import org.ehcache.clustered.client.config.DedicatedClusteredResourcePool;
import org.ehcache.clustered.client.config.SharedClusteredResourcePool;
import org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.CacheResourceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.ehcache.xml.ParsingUtil.parsePropertyOrPositiveInteger;
import static org.ehcache.xml.ParsingUtil.parsePropertyOrString;

/**
 * Provides a parser for the {@code /config/cache/resources} extension elements.
 */
@Component
public class ClusteredResourceConfigurationParser extends ClusteringParser<ResourcePool> implements CacheResourceConfigurationParser {

  public static final String SHARING_ELEMENT_NAME = "sharing";
  public static final String FROM_ELEMENT_NAME = "from";
  public static final String UNIT_ELEMENT_NAME = "unit";

  private static final String CLUSTERED_ELEMENT_NAME = "clustered";
  private static final String DEDICATED_ELEMENT_NAME = "clustered-dedicated";
  private static final String SHARED_ELEMENT_NAME = "clustered-shared";

  @Override
  public ResourcePool parse(final Element fragment, ClassLoader classLoader) {
    final String elementName = fragment.getLocalName();
    switch (elementName) {
      case SHARED_ELEMENT_NAME:
        final String sharing = parsePropertyOrString(fragment.getAttribute(SHARING_ELEMENT_NAME));
        return ClusteredResourcePoolBuilder.clusteredShared(sharing);

      case DEDICATED_ELEMENT_NAME:
        // 'from' attribute is optional on 'clustered-dedicated' element
        final Attr fromAttr = fragment.getAttributeNode(FROM_ELEMENT_NAME);
        final String from = (fromAttr == null ? null : parsePropertyOrString(fromAttr.getValue()));

        final String unitValue = fragment.getAttribute(UNIT_ELEMENT_NAME).toUpperCase();
        final MemoryUnit sizeUnits;
        try {
          sizeUnits = MemoryUnit.valueOf(unitValue);
        } catch (IllegalArgumentException e) {
          throw new XmlConfigurationException(String.format("XML configuration element <%s> 'unit' attribute '%s' is not valid", elementName, unitValue), e);
        }

        final String sizeValue;
        try {
          sizeValue = fragment.getFirstChild().getNodeValue().trim();
        } catch (DOMException e) {
          throw new XmlConfigurationException(String.format("XML configuration element <%s> value is not valid", elementName), e);
        }
        final long size;
        try {
          size = parsePropertyOrPositiveInteger(sizeValue).longValueExact();
        } catch (NumberFormatException e) {
          throw new XmlConfigurationException(String.format("XML configuration element <%s> value '%s' is not valid", elementName, sizeValue), e);
        }

        return ClusteredResourcePoolBuilder.clusteredDedicated(from, size, sizeUnits);
      case CLUSTERED_ELEMENT_NAME:
        return ClusteredResourcePoolBuilder.clustered();
      default:
        return null;
    }
  }

  @Override
  public Element safeUnparse(Document doc, ResourcePool resourcePool) {
    if (resourcePool instanceof SharedClusteredResourcePool) {
      SharedClusteredResourcePool sharedClusteredResourcePool = (SharedClusteredResourcePool) resourcePool;
      Element element = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + SHARED_ELEMENT_NAME);
      element.setAttribute(SHARING_ELEMENT_NAME, sharedClusteredResourcePool.getSharedResourcePool());
      return element;
    } else if (resourcePool instanceof DedicatedClusteredResourcePool) {
      DedicatedClusteredResourcePool dedicatedClusteredResourcePool = (DedicatedClusteredResourcePool) resourcePool;
      Element element = doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + DEDICATED_ELEMENT_NAME);
      if (dedicatedClusteredResourcePool.getFromResource() != null) {
        element.setAttribute(FROM_ELEMENT_NAME, dedicatedClusteredResourcePool.getFromResource());
      }
      element.setAttribute(UNIT_ELEMENT_NAME, dedicatedClusteredResourcePool.getUnit().toString());
      element.setTextContent(String.valueOf(dedicatedClusteredResourcePool.getSize()));
      return element;
    } else if (resourcePool instanceof ClusteredResourcePool) {
      return doc.createElementNS(NAMESPACE, TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTERED_ELEMENT_NAME);
    } else {
      return null;
    }
  }

  @Override
  public Set<Class<? extends ResourcePool>> getResourceTypes() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
      ClusteredResourcePool.class,
      DedicatedClusteredResourcePool.class,
      SharedClusteredResourcePool.class
    )));
  }
}
