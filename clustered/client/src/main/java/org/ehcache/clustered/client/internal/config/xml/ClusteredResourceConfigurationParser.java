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

import org.ehcache.clustered.client.internal.config.ClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.BaseConfigParser;
import org.ehcache.xml.CacheResourceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.osgi.service.component.annotations.Component;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.NAMESPACE;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.XML_SCHEMA;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.TC_CLUSTERED_NAMESPACE_PREFIX;

/**
 * Provides a parser for the {@code /config/cache/resources} extension elements.
 */
@Component
public class ClusteredResourceConfigurationParser extends BaseConfigParser<ResourcePool> implements CacheResourceConfigurationParser {

  private static final String CLUSTERED_ELEMENT_NAME = "clustered";
  private static final String DEDICATED_ELEMENT_NAME = "clustered-dedicated";
  private static final String SHARED_ELEMENT_NAME = "clustered-shared";
  private static final String FROM_ELEMENT_NAME = "from";
  private static final String UNIT_ELEMENT_NAME = "unit";
  private static final String SHARING_ELEMENT_NAME = "sharing";

  public ClusteredResourceConfigurationParser() {
    super(ResourcePool.class);
  }

  @Override
  public Source getXmlSchema() throws IOException {
    return new StreamSource(XML_SCHEMA.openStream());
  }

  @Override
  public URI getNamespace() {
    return NAMESPACE;
  }

  protected ResourcePool parseResourceConfig(final Element fragment) {
    final String elementName = fragment.getLocalName();
    switch (elementName) {
      case "clustered-shared":
        final String sharing = fragment.getAttribute("sharing");
        return new SharedClusteredResourcePoolImpl(sharing);

      case "clustered-dedicated":
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
      case "clustered":
        return new ClusteredResourcePoolImpl();
    }
    return null;
  }

  @Override
  public ResourcePool parseResourceConfiguration(final Element fragment) {
    ResourcePool resourcePool = parseResourceConfig(fragment);
    if (resourcePool != null) {
      return resourcePool;
    }
    throw new XmlConfigurationException(String.format("XML configuration element <%s> in <%s> is not supported",
        fragment.getTagName(), (fragment.getParentNode() == null ? "null" : fragment.getParentNode().getLocalName())));
  }

  @Override
  public Element unparseResourcePool(ResourcePool resourcePool) {
    return unparseConfig(resourcePool);
  }

  @Override
  protected Element createRootElement(Document doc, ResourcePool resourcePool) {
    Element rootElement = null;
    if (ClusteredResourcePoolImpl.class == resourcePool.getClass()) {
      rootElement = doc.createElementNS(getNamespace().toString(), TC_CLUSTERED_NAMESPACE_PREFIX + CLUSTERED_ELEMENT_NAME);
    } else if (DedicatedClusteredResourcePoolImpl.class == resourcePool.getClass()) {
      DedicatedClusteredResourcePoolImpl dedicatedClusteredResourcePool = (DedicatedClusteredResourcePoolImpl) resourcePool;
      rootElement = doc.createElementNS(getNamespace().toString(), TC_CLUSTERED_NAMESPACE_PREFIX + DEDICATED_ELEMENT_NAME);
      if (dedicatedClusteredResourcePool.getFromResource() != null) {
        rootElement.setAttribute(FROM_ELEMENT_NAME, dedicatedClusteredResourcePool.getFromResource());
      }
      rootElement.setAttribute(UNIT_ELEMENT_NAME, dedicatedClusteredResourcePool.getUnit().toString());
      rootElement.setTextContent(String.valueOf(dedicatedClusteredResourcePool.getSize()));
    } else if (SharedClusteredResourcePoolImpl.class == resourcePool.getClass()) {
      SharedClusteredResourcePoolImpl sharedClusteredResourcePool = (SharedClusteredResourcePoolImpl) resourcePool;
      rootElement = doc.createElementNS(getNamespace().toString(), TC_CLUSTERED_NAMESPACE_PREFIX + SHARED_ELEMENT_NAME);
      rootElement.setAttribute(SHARING_ELEMENT_NAME, sharedClusteredResourcePool.getSharedResourcePool());
    }
    return rootElement;
  }

  @Override
  public Set<Class<? extends ResourcePool>> getResourceTypes() {
    return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ClusteredResourcePoolImpl.class,
      DedicatedClusteredResourcePoolImpl.class, SharedClusteredResourcePoolImpl.class)));
  }
}
