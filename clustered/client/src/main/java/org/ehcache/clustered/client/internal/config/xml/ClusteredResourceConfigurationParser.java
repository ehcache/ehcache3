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

import org.ehcache.clustered.client.config.ClusteredResourceType;
import org.ehcache.clustered.client.internal.config.DedicatedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.SharedClusteredResourcePoolImpl;
import org.ehcache.clustered.client.internal.config.ClusteredResourcePoolImpl;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceType;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.CacheResourceConfigurationParser;
import org.ehcache.xml.DomUtil;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Attr;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;

import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.NAMESPACE;
import static org.ehcache.clustered.client.internal.config.xml.ClusteredCacheConstants.XML_SCHEMA;

/**
 * Provides a parser for the {@code /config/cache/resources} extension elements.
 */
public class ClusteredResourceConfigurationParser implements CacheResourceConfigurationParser {
  private static final String EHCACHE_NAMESPACE_PREFIX = "ehcache";
  private static final String RESOURCE_NAMESPACE_PREFIX = "tc";
  private static final String COLON = ":";
  private static final String CLUSTERED_ELEMENT_NAME = "clustered";
  private static final String DEDICATED_ELEMENT_NAME = "clustered-dedicated";
  private static final String SHARED_ELEMENT_NAME = "clustered-shared";
  private static final String RESOURCE_ELEMENT_NAME = "resources";
  private static final String FROM_ELEMENT_NAME = "from";
  private static final String UNIT_ELEMENT_NAME = "unit";
  private static final String SHARING_ELEMENT_NAME = "sharing";
  private static final String EHCACHE_NAMESPACE = "http://www.ehcache.org/v3";

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
    try {
      validateParametersForTranslationToResourcePool(resourcePool);
      Document doc = createDocumentRoot();
      Element rootElement = createResourceElement(doc);
      if (ClusteredResourcePoolImpl.class.isInstance(resourcePool)) {
        Element clusterElement = doc.createElement(RESOURCE_NAMESPACE_PREFIX + COLON + CLUSTERED_ELEMENT_NAME);
        clusterElement.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + RESOURCE_NAMESPACE_PREFIX, NAMESPACE
          .toString());
        rootElement.appendChild(clusterElement);
      } else if (DedicatedClusteredResourcePoolImpl.class.isInstance(resourcePool)) {
        DedicatedClusteredResourcePoolImpl dedicatedClusteredResourcePool = (DedicatedClusteredResourcePoolImpl) resourcePool;
        Element dedicatedClusterElement = doc.createElement(RESOURCE_NAMESPACE_PREFIX + COLON + DEDICATED_ELEMENT_NAME);
        dedicatedClusterElement.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + RESOURCE_NAMESPACE_PREFIX, NAMESPACE
          .toString());
        if (dedicatedClusteredResourcePool.getFromResource() != null) {
          dedicatedClusterElement.setAttribute(FROM_ELEMENT_NAME, dedicatedClusteredResourcePool.getFromResource());
        }
        dedicatedClusterElement.setAttribute(UNIT_ELEMENT_NAME, dedicatedClusteredResourcePool.getUnit().toString());
        dedicatedClusterElement.setTextContent(String.valueOf(dedicatedClusteredResourcePool.getSize()));
        rootElement.appendChild(dedicatedClusterElement);
      } else if (SharedClusteredResourcePoolImpl.class.isInstance(resourcePool)) {
        SharedClusteredResourcePoolImpl sharedClusteredResourcePool = (SharedClusteredResourcePoolImpl) resourcePool;
        Element sharedClusterElement = doc.createElement(RESOURCE_NAMESPACE_PREFIX + COLON + SHARED_ELEMENT_NAME);
        sharedClusterElement.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + RESOURCE_NAMESPACE_PREFIX, NAMESPACE
          .toString());
        sharedClusterElement.setAttribute(SHARING_ELEMENT_NAME, sharedClusteredResourcePool.getSharedResourcePool());
        rootElement.appendChild(sharedClusterElement);
      }
      return rootElement;
    } catch (SAXException | ParserConfigurationException | IOException e) {
      throw new XmlConfigurationException(e);
    }
  }

  private Element createResourceElement(Document doc)
  {
    Element rootElement = doc.createElement(EHCACHE_NAMESPACE_PREFIX + COLON + RESOURCE_ELEMENT_NAME);
    rootElement.setAttributeNS(XMLConstants.XMLNS_ATTRIBUTE_NS_URI, "xmlns:" + EHCACHE_NAMESPACE_PREFIX, EHCACHE_NAMESPACE);
    return rootElement;
  }

  private void validateParametersForTranslationToResourcePool(ResourcePool resourcePool) {
    Objects.requireNonNull(resourcePool, "ResourcePool must not be NULL");
    if (!((resourcePool instanceof ClusteredResourcePoolImpl) || (resourcePool instanceof DedicatedClusteredResourcePoolImpl) ||
          (resourcePool instanceof SharedClusteredResourcePoolImpl))) {
      throw new IllegalArgumentException("Parameter resourcePool must be type of either Clustered, Shared or Dedicated resource pool."
                                         + "Provided type of parameter is : " + resourcePool.getClass());
    }
  }

  private Document createDocumentRoot() throws IOException, SAXException, ParserConfigurationException {
    DocumentBuilder domBuilder = DomUtil.createAndGetDocumentBuilder();
    Document doc = domBuilder.newDocument();
    return doc;
  }

  @Override
  public Set<ResourceType<?>> getResourceTypes() {
    Set<ResourceType<?>> resourceType = new HashSet<>(3);
    resourceType.add(ClusteredResourceType.Types.DEDICATED);
    resourceType.add(ClusteredResourceType.Types.SHARED);
    resourceType.add(ClusteredResourceType.Types.UNKNOWN);
    return Collections.unmodifiableSet(resourceType);
  }
}
