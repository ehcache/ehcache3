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

package org.ehcache.xml.resource;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.config.SizedResourcePoolImpl;
import org.ehcache.xml.CacheResourceConfigurationParser;
import org.ehcache.xml.CoreResourceConfigurationParser;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.Disk;
import org.ehcache.xml.model.Heap;
import org.ehcache.xml.model.MemoryType;
import org.ehcache.xml.model.Offheap;
import org.ehcache.xml.model.PersistableMemoryType;
import org.ehcache.xml.model.ResourceType;
import org.w3c.dom.Element;

import java.net.URI;
import java.util.Map;

import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import static org.ehcache.xml.ConfigurationParser.CORE_SCHEMA_NAMESPACE;

public class DefaultResourceConfigurationParser implements CoreResourceConfigurationParser {

  private final Map<URI, CacheResourceConfigurationParser> extensionConfigParsers;
  private final Unmarshaller unmarshaller;

  public DefaultResourceConfigurationParser(Map<URI, CacheResourceConfigurationParser> extensionConfigParsers,
                                            Unmarshaller unmarshaller) {
    this.extensionConfigParsers = extensionConfigParsers;
    this.unmarshaller = unmarshaller;
  }

  @Override
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
    final CacheResourceConfigurationParser xmlConfigurationParser = extensionConfigParsers.get(namespace);
    if (xmlConfigurationParser == null) {
      throw new XmlConfigurationException("Can't find parser for namespace: " + namespace);
    }
    return xmlConfigurationParser.parseResourceConfiguration(element);
  }

}
