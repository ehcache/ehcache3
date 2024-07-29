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

package org.ehcache.xml;

import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import jakarta.xml.bind.helpers.DefaultValidationEventHandler;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.SizedResourcePool;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.SharedResourcePool;
import org.ehcache.impl.config.SizedResourcePoolImpl;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.Disk;
import org.ehcache.xml.model.Heap;
import org.ehcache.xml.model.MemoryTypeWithPropSubst;
import org.ehcache.xml.model.ObjectFactory;
import org.ehcache.xml.model.Offheap;
import org.ehcache.xml.model.PersistableMemoryTypeWithPropSubst;
import org.ehcache.xml.model.ResourceTypeWithPropSubst;
import org.ehcache.xml.model.ResourcesType;
import org.ehcache.xml.model.SharedDisk;
import org.ehcache.xml.model.SharedHeap;
import org.ehcache.xml.model.SharedOffheap;
import org.w3c.dom.Document;
import org.w3c.dom.DocumentFragment;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.xml.XmlConfiguration.CORE_SCHEMA_URL;
import static org.ehcache.xml.XmlUtil.newSchema;

public class ResourceConfigurationParser {

  private static final ObjectFactory OBJECT_FACTORY = new ObjectFactory();
  private static final Schema CORE_SCHEMA;
  static {
    try {
      CORE_SCHEMA = newSchema(new StreamSource(CORE_SCHEMA_URL.toExternalForm()));
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }
  private static final String CORE_SCHEMA_NS = OBJECT_FACTORY.createResource(OBJECT_FACTORY.createResourceTypeWithPropSubst()).getName().getNamespaceURI();

  private final JAXBContext jaxbContext;
  private final Set<CacheResourceConfigurationParser> extensionParsers;

  public ResourceConfigurationParser(Set<CacheResourceConfigurationParser> extensionParsers) {
    this.extensionParsers = extensionParsers;
    try {
      this.jaxbContext = JAXBContext.newInstance(ResourcesType.class);
    } catch (JAXBException e) {
      throw new AssertionError(e);
    }
  }

  public ResourcePools parse(CacheTemplate cacheTemplate, ResourcePoolsBuilder resourcePoolsBuilder, ClassLoader classLoader) {
    if (cacheTemplate.getHeap() != null) {
      return resourcePoolsBuilder.with(parseHeapConfiguration(cacheTemplate.getHeap())).build();
    } else {
      return parse(cacheTemplate.getResources(), classLoader);
    }
  }

  public ResourcePools parse(ResourcesType resources, ClassLoader classLoader) {
    if (resources == null) {
      return newResourcePoolsBuilder().build();
    } else {
      return parse(resources.getResource(), classLoader);
    }
  }

  private ResourcePools parse(List<Element> resources, ClassLoader classLoader) {
    ResourcePoolsBuilder builder = newResourcePoolsBuilder();
    for (Element element : resources) {
      ResourcePool resourcePool;
      if (!CORE_SCHEMA_NS.equals(element.getNamespaceURI())) {
        resourcePool = parseResourceExtension(element, classLoader);
      } else {
        try {
          Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
          unmarshaller.setEventHandler(new DefaultValidationEventHandler());
          Object resource = unmarshaller.unmarshal(element);
          if (resource instanceof Heap) {
            resourcePool = parseHeapConfiguration((Heap) resource);
          } else if (resource instanceof Offheap) {
            MemoryTypeWithPropSubst offheapResource = ((Offheap) resource).getValue();
            resourcePool = new SizedResourcePoolImpl<>(org.ehcache.config.ResourceType.Core.OFFHEAP,
              offheapResource.getValue().longValue(), parseMemory(offheapResource), false);
          } else if (resource instanceof Disk) {
            PersistableMemoryTypeWithPropSubst diskResource = ((Disk) resource).getValue();
            resourcePool = new SizedResourcePoolImpl<>(org.ehcache.config.ResourceType.Core.DISK,
              diskResource.getValue().longValue(), parseMemory(diskResource), diskResource.isPersistent());
          } else if (resource instanceof SharedHeap) {
            resourcePool = new SharedResourcePool<>(org.ehcache.config.ResourceType.Core.HEAP, false);
          } else if (resource instanceof SharedOffheap) {
            resourcePool = new SharedResourcePool<>(org.ehcache.config.ResourceType.Core.OFFHEAP, false);
          } else if (resource instanceof SharedDisk) {
            resourcePool = new SharedResourcePool<>(ResourceType.Core.DISK, false);
          } else {
            // Someone updated the core resources without updating *this* code ...
            throw new AssertionError("Unrecognized resource: " + element + " / " + resource.getClass().getName());
          }
        } catch (JAXBException e) {
          throw new IllegalArgumentException("Can't find parser for resource: " + element, e);
        }
      }

      builder = builder.with(resourcePool);
    }
    return builder.build();
  }

  private ResourcePool parseHeapConfiguration(Heap heap) {
    ResourceTypeWithPropSubst heapResource = heap.getValue();
    return new SizedResourcePoolImpl<>(org.ehcache.config.ResourceType.Core.HEAP,
      heapResource.getValue().longValue(), parseUnit(heapResource), false);
  }

  private static ResourceUnit parseUnit(ResourceTypeWithPropSubst resourceType) {
    if (resourceType.getUnit().equals(org.ehcache.xml.model.ResourceUnit.ENTRIES)) {
      return EntryUnit.ENTRIES;
    } else {
      return org.ehcache.config.units.MemoryUnit.valueOf(resourceType.getUnit().value().toUpperCase());
    }
  }

  private static org.ehcache.config.units.MemoryUnit parseMemory(MemoryTypeWithPropSubst memoryType) {
    return MemoryUnit.valueOf(memoryType.getUnit().value().toUpperCase());
  }

  ResourcePool parseResourceExtension(final Element element, ClassLoader classLoader) {
    for (CacheResourceConfigurationParser parser : extensionParsers) {
      ResourcePool resourcePool = parser.parse(element, classLoader);
      if (resourcePool != null) {
        return resourcePool;
      }
    }
    throw new XmlConfigurationException("Can't find parser for element: " + element);
  }

  public ResourcesType unparse(Document target, ResourcePools resourcePools) {
    List<Element> resources = new ArrayList<>();
    resourcePools.getResourceTypeSet().forEach(resourceType -> {
      ResourcePool resourcePool = resourcePools.getPoolForResource(resourceType);
      if (resourceType instanceof org.ehcache.config.ResourceType.Core) {
        SizedResourcePool pool = (SizedResourcePool) resourcePool;
        Object resource;
        if (resourceType == org.ehcache.config.ResourceType.Core.HEAP) {
          resource = OBJECT_FACTORY.createHeap(OBJECT_FACTORY.createResourceTypeWithPropSubst().withValue(BigInteger.valueOf(pool.getSize())).withUnit(unparseUnit(pool.getUnit())));
        } else if (resourceType == org.ehcache.config.ResourceType.Core.OFFHEAP) {
          resource = OBJECT_FACTORY.createOffheap(OBJECT_FACTORY.createMemoryTypeWithPropSubst().withValue(BigInteger.valueOf(pool.getSize())).withUnit(unparseMemory((MemoryUnit) pool.getUnit())));
        } else if (resourceType == org.ehcache.config.ResourceType.Core.DISK) {
          resource = OBJECT_FACTORY.createDisk(OBJECT_FACTORY.createPersistableMemoryTypeWithPropSubst().withValue(BigInteger.valueOf(pool.getSize()))
            .withUnit(unparseMemory((MemoryUnit) pool.getUnit())).withPersistent(pool.isPersistent()));
        } else {
          throw new AssertionError("Unrecognized core resource type: " + resourceType);
        }
        addResourceElementForUnparse(target, resources, resource);
      } else if (resourceType instanceof org.ehcache.config.ResourceType.SharedResource<?>) {
        ResourceType<?> sharing = ((ResourceType.SharedResource<?>) resourceType).getResourceType();
        if (sharing instanceof org.ehcache.config.ResourceType.Core) {
          switch ((org.ehcache.config.ResourceType.Core) sharing) {
            case HEAP:
              addResourceElementForUnparse(target, resources, OBJECT_FACTORY.createSharedHeap(""));
              break;
            case OFFHEAP:
              addResourceElementForUnparse(target, resources, OBJECT_FACTORY.createSharedOffheap(""));
              break;
            case DISK:
              addResourceElementForUnparse(target, resources, OBJECT_FACTORY.createSharedDisk(""));
              break;
            default:
              throw new AssertionError("Parser not found for resource type: " + resourceType);
          }
        } else {
          Map<Class<? extends ResourcePool>, CacheResourceConfigurationParser> parsers = new HashMap<>();
          extensionParsers.forEach(parser -> parser.getResourceTypes().forEach(rt -> parsers.put(rt, parser)));
          CacheResourceConfigurationParser parser = parsers.get(resourcePool.getClass());
          if (parser != null) {
            resources.add(parser.unparse(target, resourcePool));
          }
          else {
            throw new AssertionError("Parser not found for resource type: " + resourceType);
          }
        }
      } else {
        Map<Class<? extends ResourcePool>, CacheResourceConfigurationParser> parsers = new HashMap<>();
        extensionParsers.forEach(parser -> parser.getResourceTypes().forEach(rt -> parsers.put(rt, parser)));
        CacheResourceConfigurationParser parser = parsers.get(resourcePool.getClass());
        if (parser != null) {
          resources.add(parser.unparse(target, resourcePool));
        } else {
          throw new AssertionError("Parser not found for resource type: " + resourceType);
        }
      }
    });
    return OBJECT_FACTORY.createResourcesType().withResource(resources);
  }

  private void addResourceElementForUnparse(Document target, List<Element> resources, Object resource) {
    try {
      DocumentFragment fragment = target.createDocumentFragment();
      Marshaller marshaller = jaxbContext.createMarshaller();
      marshaller.setSchema(CORE_SCHEMA);
      marshaller.marshal(resource, fragment);
      NodeList children = fragment.getChildNodes();
      for (int i = 0; i < children.getLength(); i++) {
        Node item = children.item(i);
        if (item instanceof Element) {
          resources.add((Element) item);
        } else {
          throw new XmlConfigurationException("Unexpected marshalled resource node: " + item);
        }
      }
    } catch (JAXBException e) {
      throw new XmlConfigurationException(e);
    }
  }

  private static org.ehcache.xml.model.ResourceUnit unparseUnit(ResourceUnit resourceUnit) {
    if (resourceUnit instanceof EntryUnit) {
      return org.ehcache.xml.model.ResourceUnit.ENTRIES;
    } else {
      return org.ehcache.xml.model.ResourceUnit.fromValue(resourceUnit.toString());
    }
  }

  private static org.ehcache.xml.model.MemoryUnit unparseMemory(MemoryUnit memoryUnit) {
    return org.ehcache.xml.model.MemoryUnit.fromValue(memoryUnit.toString());
  }

}
