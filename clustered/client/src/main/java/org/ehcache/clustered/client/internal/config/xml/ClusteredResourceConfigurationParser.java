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

import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.ClusteredResourcePoolImpl;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.CacheResourceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.NAMESPACE;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.XML_SCHEMA;

/**
 * Provides a parser for the {@code /config/cache/resources} extension elements.
 */
public class ClusteredResourceConfigurationParser implements CacheResourceConfigurationParser {
  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  @Override
  public ResourcePool parseResourceConfiguration(final Element fragment) {
    final String elementName = fragment.getLocalName();
    if ("clustered-shared".equals(elementName)) {
      final String sharing = fragment.getAttribute("sharing");
      return new SharedClusteredResourcePoolImpl(sharing);

    } else if ("clustered-dedicated".equals(elementName)) {
      // 'from' attribute is optional on 'clustered-dedicated' element
      final Attr fromAttr = fragment.getAttributeNode("from");
      final String from = (fromAttr == null ? null : fromAttr.getValue());

      final String unitValue = fragment.getAttribute("unit").toUpperCase();
      final MemoryUnit sizeUnits;
      try {
        sizeUnits = MemoryUnit.valueOf(unitValue);
      } catch (IllegalArgumentException e) {
        throw new XmlConfigurationException(String.format("XML configuration element <%s> 'unit' attribute '%s' is not valid", elementName, unitValue), e);
      }

      final String sizeValue;
      try {
        sizeValue = fragment.getFirstChild().getNodeValue();
      } catch (DOMException e) {
        throw new XmlConfigurationException(String.format("XML configuration element <%s> value is not valid", elementName), e);
      }
      final long size;
      try {
        size = Long.parseLong(sizeValue);
      } catch (NumberFormatException e) {
        throw new XmlConfigurationException(String.format("XML configuration element <%s> value '%s' is not valid", elementName, sizeValue), e);
      }

      return new DedicatedClusteredResourcePoolImpl(from, size, sizeUnits);
    } else if("clustered".equals(elementName)) {
      return new ClusteredResourcePoolImpl();
    }

    throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
  }
}
