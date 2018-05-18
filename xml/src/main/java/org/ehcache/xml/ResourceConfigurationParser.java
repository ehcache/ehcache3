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
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.SizedResourcePoolImpl;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.Disk;
import org.ehcache.xml.model.Heap;
import org.ehcache.xml.model.MemoryType;
import org.ehcache.xml.model.Offheap;
import org.ehcache.xml.model.PersistableMemoryType;
import org.ehcache.xml.model.ResourceType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.ParserConfigurationException;

import static org.ehcache.xml.ConfigurationParser.CORE_SCHEMA_NAMESPACE;

public class ResourceConfigurationParser {

  private final Map<org.ehcache.config.ResourceType<?>, CacheResourceConfigurationParser> extensionResourceParsers = new HashMap<>();
  private final Map<URI, CacheResourceConfigurationParser> extensionUriParsers = new HashMap<>();
  private final Marshaller marshaller;
  private final Unmarshaller unmarshaller;

  public ResourceConfigurationParser(Marshaller marshaller, Unmarshaller unmarshaller) {
    for (CacheResourceConfigurationParser parser : ClassLoading.libraryServiceLoaderFor(CacheResourceConfigurationParser.class)) {
      extensionUriParsers.put(parser.getNamespace(), parser);
      parser.getResourceTypes().forEach(resourceType -> {
        extensionResourceParsers.put(resourceType, parser);
      });
    }

    this.marshaller = marshaller;
    this.unmarshaller = unmarshaller;
  }

  public ResourcePool parseResourceConfiguration(Element element) {
    if (!CORE_SCHEMA_NAMESPACE.equals(element.getNamespaceURI())) {
      return parseResourceExtension(element);
    }
    try {
      Object resource = unmarshaller.unmarshal(element);
      if (resource instanceof Heap) {
        ResourceType heapResource = ((Heap) resource).getValue();
        return new SizedResourcePoolImpl<>(org.ehcache.config.ResourceType.Core.HEAP,
          heapResource.getValue().longValue(), parseUnit(heapResource), false);
      } else if (resource instanceof Offheap) {
        MemoryType offheapResource = ((Offheap) resource).getValue();
        return new SizedResourcePoolImpl<>(org.ehcache.config.ResourceType.Core.OFFHEAP,
          offheapResource.getValue().longValue(), parseMemory(offheapResource), false);
      } else if (resource instanceof Disk) {
        PersistableMemoryType diskResource = ((Disk) resource).getValue();
        return new SizedResourcePoolImpl<>(org.ehcache.config.ResourceType.Core.DISK,
          diskResource.getValue().longValue(), parseMemory(diskResource), diskResource.isPersistent());
      } else {
        // Someone updated the core resources without updating *this* code ...
        throw new AssertionError("Unrecognized resource: " + element + " / " + resource.getClass().getName());
      }
    } catch (JAXBException e) {
      throw new IllegalArgumentException("Can't find parser for resource: " + element, e);
    }
  }

  private static ResourceUnit parseUnit(ResourceType resourceType) {
    if (resourceType.getUnit().value().equalsIgnoreCase("entries")) {
      return EntryUnit.ENTRIES;
    } else {
      return org.ehcache.config.units.MemoryUnit.valueOf(resourceType.getUnit().value().toUpperCase());
    }
  }

  private static org.ehcache.config.units.MemoryUnit parseMemory(MemoryType memoryType) {
    return MemoryUnit.valueOf(memoryType.getUnit().value().toUpperCase());
  }

  ResourcePool parseResourceExtension(final Element element) {
    URI namespace = URI.create(element.getNamespaceURI());
    final CacheResourceConfigurationParser xmlConfigurationParser = extensionUriParsers.get(namespace);
    if (xmlConfigurationParser == null) {
      throw new XmlConfigurationException("Can't find parser for namespace: " + namespace);
    }
    return xmlConfigurationParser.parseResourceConfiguration(element);
  }

  public Element unparseResourceConfiguration(ResourcePool resourcePool) {
    if (resourcePool.getType() instanceof org.ehcache.config.ResourceType.Core) {
      SizedResourcePool pool = (SizedResourcePool) resourcePool;
      Object resource = null;
      if (resourcePool.getType() == org.ehcache.config.ResourceType.Core.HEAP) {
        Heap heap = new Heap();
        ResourceType resourceType = new ResourceType();
        resourceType.setValue(BigInteger.valueOf(pool.getSize()));
        ResourceUnit resourceUnit = pool.getUnit();
        resourceType.setUnit(unparseUnit(resourceUnit));
        heap.setValue(resourceType);
        resource = heap;
      } else if (resourcePool.getType() == org.ehcache.config.ResourceType.Core.OFFHEAP) {
        Offheap offheap = new Offheap();
        MemoryType memoryType = new MemoryType();
        memoryType.setValue(BigInteger.valueOf(pool.getSize()));
        memoryType.setUnit(unparseMemory((MemoryUnit) pool.getUnit()));
        offheap.setValue(memoryType);
        resource = offheap;
      } else if (resourcePool.getType() == org.ehcache.config.ResourceType.Core.DISK) {
        Disk disk = new Disk();
        PersistableMemoryType memoryType = new PersistableMemoryType();
        memoryType.setValue(BigInteger.valueOf(pool.getSize()));
        memoryType.setUnit(unparseMemory((MemoryUnit) pool.getUnit()));
        memoryType.setPersistent(pool.isPersistent());
        disk.setValue(memoryType);
        resource = disk;
      } else {
        throw new AssertionError("Unrecognized core resource type: " + resourcePool.getType());
      }
      try {
        Document document = DomUtil.createAndGetDocumentBuilder().newDocument();
        marshaller.marshal(resource, document);
        return document.getDocumentElement();
      } catch (SAXException | ParserConfigurationException | IOException | JAXBException e) {
        throw new XmlConfigurationException(e);
      }
    } else {
      CacheResourceConfigurationParser parser = extensionResourceParsers.get(resourcePool.getType());
      if (parser != null) {
        return parser.unparseResourcePool(resourcePool);
      } else {
        throw new AssertionError("Parser not found for resource type: " + resourcePool.getType());
      }
    }
  }

  private static org.ehcache.xml.model.ResourceUnit unparseUnit(ResourceUnit resourceUnit) {
    if (resourceUnit instanceof EntryUnit) {
      return org.ehcache.xml.model.ResourceUnit.ENTRIES;
    } else {
      return org.ehcache.xml.model.ResourceUnit.valueOf(((MemoryUnit) resourceUnit).name());
    }
  }

  private static org.ehcache.xml.model.MemoryUnit unparseMemory(MemoryUnit memoryUnit) {
    return org.ehcache.xml.model.MemoryUnit.fromValue(memoryUnit.name());
  }

}
