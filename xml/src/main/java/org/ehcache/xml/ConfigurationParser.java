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
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.BaseCacheType;
import org.ehcache.xml.model.CacheLoaderWriterType;
import org.ehcache.xml.model.CacheTemplateType;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.CopierType;
import org.ehcache.xml.model.Disk;
import org.ehcache.xml.model.DiskStoreSettingsType;
import org.ehcache.xml.model.EventFiringType;
import org.ehcache.xml.model.EventOrderingType;
import org.ehcache.xml.model.EventType;
import org.ehcache.xml.model.ExpiryType;
import org.ehcache.xml.model.Heap;
import org.ehcache.xml.model.ListenersType;
import org.ehcache.xml.model.MemoryType;
import org.ehcache.xml.model.ObjectFactory;
import org.ehcache.xml.model.Offheap;
import org.ehcache.xml.model.PersistableMemoryType;
import org.ehcache.xml.model.PersistenceType;
import org.ehcache.xml.model.ResourceType;
import org.ehcache.xml.model.ResourcesType;
import org.ehcache.xml.model.SerializerType;
import org.ehcache.xml.model.ServiceType;
import org.ehcache.xml.model.SizeofType;
import org.ehcache.xml.model.TimeType;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.core.internal.util.ClassLoading;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.ehcache.xml.model.ThreadPoolReferenceType;
import org.ehcache.xml.model.ThreadPoolsType;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * Provides support for parsing a cache configuration expressed in XML.
 */
class ConfigurationParser {

  private static final Pattern SYSPROP = Pattern.compile("\\$\\{([^}]+)\\}");
  private static final SchemaFactory XSD_SCHEMA_FACTORY = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
  private static Schema newSchema(Source[] schemas) throws SAXException {
    synchronized (XSD_SCHEMA_FACTORY) {
      return XSD_SCHEMA_FACTORY.newSchema(schemas);
    }
  }

  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");
  private static final String CORE_SCHEMA_NAMESPACE = "http://www.ehcache.org/v3";
  private static final String CORE_SCHEMA_ROOT_ELEMENT = "config";
  private static final String CORE_SCHEMA_JAXB_MODEL_PACKAGE = ConfigType.class.getPackage().getName();

  private final Map<URI, CacheManagerServiceConfigurationParser<?>> xmlParsers = new HashMap<URI, CacheManagerServiceConfigurationParser<?>>();
  private final Map<URI, CacheServiceConfigurationParser<?>> cacheXmlParsers = new HashMap<URI, CacheServiceConfigurationParser<?>>();
  private final Unmarshaller unmarshaller;
  private final Map<URI, CacheResourceConfigurationParser> resourceXmlParsers = new HashMap<URI, CacheResourceConfigurationParser>();
  private final ConfigType config;

  static String replaceProperties(String originalValue, final Properties properties) {
    Matcher matcher = SYSPROP.matcher(originalValue);

    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      final String property = matcher.group(1);
      final String value = properties.getProperty(property);
      if (value == null) {
        throw new IllegalStateException(String.format("Replacement for ${%s} not found!", property));
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(sb);
    final String resolvedValue = sb.toString();
    return resolvedValue.equals(originalValue) ? null : resolvedValue;
  }

  public ConfigurationParser(String xml) throws IOException, SAXException, JAXBException, ParserConfigurationException {
    Collection<Source> schemaSources = new ArrayList<Source>();
    schemaSources.add(new StreamSource(CORE_SCHEMA_URL.openStream()));

    for (CacheManagerServiceConfigurationParser<?> parser : ClassLoading.libraryServiceLoaderFor(CacheManagerServiceConfigurationParser.class)) {
      schemaSources.add(parser.getXmlSchema());
      xmlParsers.put(parser.getNamespace(), parser);
    }
    for (CacheServiceConfigurationParser<?> parser : ClassLoading.libraryServiceLoaderFor(CacheServiceConfigurationParser.class)) {
      schemaSources.add(parser.getXmlSchema());
      cacheXmlParsers.put(parser.getNamespace(), parser);
    }
    // Parsers for /config/cache/resources extensions
    for (CacheResourceConfigurationParser parser : ClassLoading.libraryServiceLoaderFor(CacheResourceConfigurationParser.class)) {
      schemaSources.add(parser.getXmlSchema());
      resourceXmlParsers.put(parser.getNamespace(), parser);
    }

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);
    factory.setSchema(newSchema(schemaSources.toArray(new Source[schemaSources.size()])));

    DocumentBuilder domBuilder = factory.newDocumentBuilder();
    domBuilder.setErrorHandler(new FatalErrorHandler());
    Element dom = domBuilder.parse(xml).getDocumentElement();

    substituteSystemProperties(dom);

    if (!CORE_SCHEMA_ROOT_ELEMENT.equals(dom.getLocalName()) || !CORE_SCHEMA_NAMESPACE.equals(dom.getNamespaceURI())) {
      throw new XmlConfigurationException("Expecting {" + CORE_SCHEMA_NAMESPACE + "}" + CORE_SCHEMA_ROOT_ELEMENT
          + " element; found {" + dom.getNamespaceURI() + "}" + dom.getLocalName());
    }

    Class<ConfigType> configTypeClass = ConfigType.class;
    JAXBContext jc = JAXBContext.newInstance(CORE_SCHEMA_JAXB_MODEL_PACKAGE, configTypeClass.getClassLoader());
    this.unmarshaller = jc.createUnmarshaller();
    this.config = unmarshaller.unmarshal(dom, configTypeClass).getValue();
  }

  private void substituteSystemProperties(final Element dom) {
    final Properties properties = System.getProperties();
    Stack<NodeList> nodeLists = new Stack<NodeList>();
    nodeLists.push(dom.getChildNodes());
    while (!nodeLists.isEmpty()) {
      NodeList nodeList = nodeLists.pop();
      for (int i = 0; i < nodeList.getLength(); ++i) {
        Node currentNode = nodeList.item(i);
        if (currentNode.hasChildNodes()) {
          nodeLists.push(currentNode.getChildNodes());
        }
        final NamedNodeMap attributes = currentNode.getAttributes();
        if (attributes != null) {
          for (int j = 0; j < attributes.getLength(); ++j) {
            final Node attributeNode = attributes.item(j);
            final String newValue = replaceProperties(attributeNode.getNodeValue(), properties);
            if (newValue != null) {
              attributeNode.setNodeValue(newValue);
            }
          }
        }
        if (currentNode.getNodeType() == Node.TEXT_NODE) {
          final String newValue = replaceProperties(currentNode.getNodeValue(), properties);
          if (newValue != null) {
            currentNode.setNodeValue(newValue);
          }
        }
      }
    }
  }

  public Iterable<ServiceType> getServiceElements() {
    return config.getService();
  }

  public SerializerType getDefaultSerializers() {
    return config.getDefaultSerializers();
  }

  public CopierType getDefaultCopiers() {
    return config.getDefaultCopiers();
  }

  public PersistenceType getPersistence() {
    return config.getPersistence();
  }

  public ThreadPoolReferenceType getEventDispatch() {
    return config.getEventDispatch();
  }

  public ThreadPoolReferenceType getWriteBehind() {
    return config.getWriteBehind();
  }

  public ThreadPoolReferenceType getDiskStore() {
    return config.getDiskStore();
  }

  public ThreadPoolsType getThreadPools() {
    return config.getThreadPools();
  }

  public SizeOfEngineLimits getHeapStore() {
    SizeofType type = config.getHeapStore();
    return type == null ? null : new XmlSizeOfEngineLimits(type);
  }

  public Iterable<CacheDefinition> getCacheElements() {
    List<CacheDefinition> cacheCfgs = new ArrayList<CacheDefinition>();
    final List<BaseCacheType> cacheOrCacheTemplate = config.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if(baseCacheType instanceof CacheType) {
        final CacheType cacheType = (CacheType)baseCacheType;

        final BaseCacheType[] sources;
        if(cacheType.getUsesTemplate() != null) {
          sources = new BaseCacheType[2];
          sources[0] = cacheType;
          sources[1] = (BaseCacheType) cacheType.getUsesTemplate();
        } else {
          sources = new BaseCacheType[1];
          sources[0] = cacheType;
        }

        cacheCfgs.add(new CacheDefinition() {
          @Override
          public String id() {
            return cacheType.getAlias();
          }

          @Override
          public String keyType() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getKeyType() != null ? source.getKeyType().getValue() : null;
              if (value != null) break;
            }
            if (value == null) {
              for (BaseCacheType source : sources) {
                value = JaxbHelper.findDefaultValue(source, "keyType");
                if (value != null) break;
              }
            }
            return value;
          }

          @Override
          public String keySerializer() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getKeyType() != null ? source.getKeyType().getSerializer() : null;
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String keyCopier() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getKeyType() != null ? source.getKeyType().getCopier() : null;
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String valueType() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getValueType() != null ? source.getValueType().getValue() : null;
              if (value != null) break;
            }
            if (value == null) {
              for (BaseCacheType source : sources) {
                value = JaxbHelper.findDefaultValue(source, "valueType");
                if (value != null) break;
              }
            }
            return value;
          }

          @Override
          public String valueSerializer() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getValueType() != null ? source.getValueType().getSerializer() : null;
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String valueCopier() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getValueType() != null ? source.getValueType().getCopier() : null;
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String evictionAdvisor() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getEvictionAdvisor();
              if (value != null) break;
            }
            return value;
          }

          @Override
          public Expiry expiry() {
            ExpiryType value = null;
            for (BaseCacheType source : sources) {
              value = source.getExpiry();
              if (value != null) break;
            }
            if (value != null) {
              return new XmlExpiry(value);
            } else {
              return null;
            }
          }

          @Override
          public String loaderWriter() {
            String configClass = null;
            for (BaseCacheType source : sources) {
              final CacheLoaderWriterType loaderWriter = source.getLoaderWriter();
              if (loaderWriter != null) {
                configClass = loaderWriter.getClazz();
                break;
              }
            }
            return configClass;
          }

          @Override
          public ListenersConfig listenersConfig() {
            ListenersType base = null;
            ArrayList<ListenersType> additionals = new ArrayList<ListenersType>();
            for (BaseCacheType source : sources) {
              if (source.getListeners() != null) {
                if (base == null) {
                  base = source.getListeners();
                } else {
                  additionals.add(source.getListeners());
                }
              }
            }
            return base != null ? new XmlListenersConfig(base, additionals.toArray(new ListenersType[0])) : null;
          }


          @Override
          public Iterable<ServiceConfiguration<?>> serviceConfigs() {
            Map<Class<? extends ServiceConfiguration>, ServiceConfiguration<?>> configsMap =
                new HashMap<Class<? extends ServiceConfiguration>, ServiceConfiguration<?>>();
            for (BaseCacheType source : sources) {
              for (Element child : source.getServiceConfiguration()) {
                ServiceConfiguration<?> serviceConfiguration = parseCacheExtension(child);
                if (!configsMap.containsKey(serviceConfiguration.getClass())) {
                  configsMap.put(serviceConfiguration.getClass(), serviceConfiguration);
                }
              }
            }
            return configsMap.values();
          }

          @Override
          public Collection<ResourcePool> resourcePools() {
            for (BaseCacheType source : sources) {
              Heap heapResource = source.getHeap();
              if (heapResource != null) {
                return singleton(parseResource(heapResource));
              } else {
                ResourcesType resources = source.getResources();
                if (resources != null) {
                  return parseResources(resources);
                }
              }
            }
            return emptySet();
          }

          @Override
          public WriteBehind writeBehind() {
            for (BaseCacheType source : sources) {
              final CacheLoaderWriterType loaderWriter = source.getLoaderWriter();
              final CacheLoaderWriterType.WriteBehind writebehind = loaderWriter != null ? loaderWriter.getWriteBehind() : null;
              if (writebehind != null) {
                return new XmlWriteBehind(writebehind);
              }
            }
            return null;
          }

          @Override
          public DiskStoreSettings diskStoreSettings() {
            DiskStoreSettingsType value = null;
            for (BaseCacheType source : sources) {
              value = source.getDiskStoreSettings();
              if (value != null) break;
            }
            if (value != null) {
              return new XmlDiskStoreSettings(value);
            } else {
              return null;
            }
          }

          @Override
          public SizeOfEngineLimits heapStoreSettings() {
            SizeofType sizeofType = null;
            for (BaseCacheType source : sources) {
              sizeofType = source.getHeapStoreSettings();
              if (sizeofType != null) break;
            }
            return sizeofType != null ? new XmlSizeOfEngineLimits(sizeofType) : null;
          }
        });
      }
    }

    return Collections.unmodifiableList(cacheCfgs);
  }

  public Map<String, CacheTemplate> getTemplates() {
    final Map<String, CacheTemplate> templates = new HashMap<String, CacheTemplate>();
    final List<BaseCacheType> cacheOrCacheTemplate = config.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if (baseCacheType instanceof CacheTemplateType) {
        final CacheTemplateType cacheTemplate = (CacheTemplateType)baseCacheType;
        templates.put(cacheTemplate.getName(), new CacheTemplate() {

          @Override
          public String keyType() {
            String keyType = cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getValue() : null;
            if (keyType == null) {
              keyType = JaxbHelper.findDefaultValue(cacheTemplate, "keyType");
            }
            return keyType;
          }

          @Override
          public String keySerializer() {
            return cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getSerializer() : null;
          }

          @Override
          public String keyCopier() {
            return cacheTemplate.getKeyType() != null ? cacheTemplate.getKeyType().getCopier() : null;
          }

          @Override
          public String valueType() {
            String valueType = cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getValue() : null;
            if (valueType == null) {
              valueType = JaxbHelper.findDefaultValue(cacheTemplate, "valueType");
            }
            return valueType;
          }

          @Override
          public String valueSerializer() {
            return cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getSerializer() : null;
          }

          @Override
          public String valueCopier() {
            return cacheTemplate.getValueType() != null ? cacheTemplate.getValueType().getCopier() : null;
          }

          @Override
          public String evictionAdvisor() {
            return cacheTemplate.getEvictionAdvisor();
          }

          @Override
          public Expiry expiry() {
            ExpiryType cacheTemplateExpiry = cacheTemplate.getExpiry();
            if (cacheTemplateExpiry != null) {
              return new XmlExpiry(cacheTemplateExpiry);
            } else {
              return null;
            }
          }

          @Override
          public ListenersConfig listenersConfig() {
            final ListenersType integration = cacheTemplate.getListeners();
            return integration != null ? new XmlListenersConfig(integration) : null;
          }

          @Override
          public String loaderWriter() {
            final CacheLoaderWriterType loaderWriter = cacheTemplate.getLoaderWriter();
            return loaderWriter != null ? loaderWriter.getClazz() : null;
          }

          @Override
          public Iterable<ServiceConfiguration<?>> serviceConfigs() {
            Collection<ServiceConfiguration<?>> configs = new ArrayList<ServiceConfiguration<?>>();
            for (Element child : cacheTemplate.getServiceConfiguration()) {
              configs.add(parseCacheExtension(child));
            }
            return configs;
          }

          @Override
          public Collection<ResourcePool> resourcePools() {
            Heap heapResource = cacheTemplate.getHeap();
            if (heapResource != null) {
              return singleton(parseResource(heapResource));
            } else {
              ResourcesType resources = cacheTemplate.getResources();
              if (resources != null) {
                return parseResources(resources);
              }
            }

            return emptySet();
          }

          @Override
          public WriteBehind writeBehind() {
            final CacheLoaderWriterType loaderWriter = cacheTemplate.getLoaderWriter();
            final CacheLoaderWriterType.WriteBehind writebehind = loaderWriter != null ? loaderWriter.getWriteBehind(): null;
            return writebehind != null ? new XmlWriteBehind(writebehind) : null;
          }

          @Override
          public DiskStoreSettings diskStoreSettings() {
            final DiskStoreSettingsType diskStoreSettings = cacheTemplate.getDiskStoreSettings();
            return diskStoreSettings == null ? null : new XmlDiskStoreSettings(diskStoreSettings);
          }

          @Override
          public SizeOfEngineLimits heapStoreSettings() {
            SizeofType type = cacheTemplate.getHeapStoreSettings();
            return type == null ? null : new XmlSizeOfEngineLimits(type);
          }
        });
      }
    }
    return Collections.unmodifiableMap(templates);
  }

  private Collection<ResourcePool> parseResources(ResourcesType resources) {
    Collection<ResourcePool> resourcePools = new ArrayList<ResourcePool>();
    for (Element resource : resources.getResource()) {
      resourcePools.add(parseResource(resource));
    }
    return resourcePools;
  }

  private ResourcePool parseResource(Heap resource) {
    ResourceType heapResource = resource.getValue();
    return new SizedResourcePoolImpl<SizedResourcePool>(org.ehcache.config.ResourceType.Core.HEAP,
            heapResource.getValue().longValue(), parseUnit(heapResource), false);
  }

  private ResourcePool parseResource(Element element) {
    if (!CORE_SCHEMA_NAMESPACE.equals(element.getNamespaceURI())) {
      return parseResourceExtension(element);
    }
    try {
      Object resource = unmarshaller.unmarshal(element);
      if (resource instanceof Heap) {
        ResourceType heapResource = ((Heap) resource).getValue();
        return new SizedResourcePoolImpl<SizedResourcePool>(org.ehcache.config.ResourceType.Core.HEAP,
                heapResource.getValue().longValue(), parseUnit(heapResource), false);
      } else if (resource instanceof Offheap) {
        MemoryType offheapResource = ((Offheap) resource).getValue();
        return new SizedResourcePoolImpl<SizedResourcePool>(org.ehcache.config.ResourceType.Core.OFFHEAP,
                offheapResource.getValue().longValue(), parseMemory(offheapResource), false);
      } else if (resource instanceof Disk) {
        PersistableMemoryType diskResource = ((Disk) resource).getValue();
        return new SizedResourcePoolImpl<SizedResourcePool>(org.ehcache.config.ResourceType.Core.DISK,
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
      return MemoryUnit.valueOf(resourceType.getUnit().value().toUpperCase());
    }
  }

  private static MemoryUnit parseMemory(MemoryType memoryType) {
    return MemoryUnit.valueOf(memoryType.getUnit().value().toUpperCase());
  }

  ServiceCreationConfiguration<?> parseExtension(final Element element) {
    URI namespace = URI.create(element.getNamespaceURI());
    final CacheManagerServiceConfigurationParser<?> cacheManagerServiceConfigurationParser = xmlParsers.get(namespace);
    if(cacheManagerServiceConfigurationParser == null) {
      throw new IllegalArgumentException("Can't find parser for namespace: " + namespace);
    }
    return cacheManagerServiceConfigurationParser.parseServiceCreationConfiguration(element);
  }

  ServiceConfiguration<?> parseCacheExtension(final Element element) {
    URI namespace = URI.create(element.getNamespaceURI());
    final CacheServiceConfigurationParser<?> xmlConfigurationParser = cacheXmlParsers.get(namespace);
    if(xmlConfigurationParser == null) {
      throw new IllegalArgumentException("Can't find parser for namespace: " + namespace);
    }
    return xmlConfigurationParser.parseServiceConfiguration(element);
  }

  ResourcePool parseResourceExtension(final Element element) {
    URI namespace = URI.create(element.getNamespaceURI());
    final CacheResourceConfigurationParser xmlConfigurationParser = resourceXmlParsers.get(namespace);
    if (xmlConfigurationParser == null) {
      throw new XmlConfigurationException("Can't find parser for namespace: " + namespace);
    }
    return xmlConfigurationParser.parseResourceConfiguration(element);
  }

  static class FatalErrorHandler implements ErrorHandler {

    @Override
    public void warning(SAXParseException exception) throws SAXException {
      throw exception;
    }

    @Override
    public void error(SAXParseException exception) throws SAXException {
      throw exception;
    }

    @Override
    public void fatalError(SAXParseException exception) throws SAXException {
      throw exception;
    }
  }

  interface CacheTemplate {

    String keyType();

    String keySerializer();

    String keyCopier();

    String valueType();

    String valueSerializer();

    String valueCopier();

    String evictionAdvisor();

    Expiry expiry();

    String loaderWriter();

    ListenersConfig listenersConfig();

    Iterable<ServiceConfiguration<?>> serviceConfigs();

    Collection<ResourcePool> resourcePools();

    WriteBehind writeBehind();

    DiskStoreSettings diskStoreSettings();

    SizeOfEngineLimits heapStoreSettings();

  }

  interface CacheDefinition extends CacheTemplate {

    String id();

  }

  interface ListenersConfig {

    int dispatcherConcurrency();

    String threadPool();

    Iterable<Listener> listeners();
  }

  interface Listener {

    String className();

    EventFiringType eventFiring();

    EventOrderingType eventOrdering();

    List<EventType> fireOn();

  }

  interface Expiry {

    boolean isUserDef();

    boolean isTTI();

    boolean isTTL();

    String type();

    long value();

    TimeUnit unit();

  }

  interface WriteBehind {

    int maxQueueSize();

    int concurrency();

    String threadPool();

    Batching batching();
  }

  interface Batching {

    boolean isCoalesced();

    int batchSize();

    long maxDelay();

    TimeUnit maxDelayUnit();
  }

  interface DiskStoreSettings {

    int writerConcurrency();

    String threadPool();
  }


  interface SizeOfEngineLimits {

    long getMaxObjectGraphSize();

    long getMaxObjectSize();

    MemoryUnit getUnit();
  }

  private static class XmlListenersConfig implements ListenersConfig {

    final int dispatcherConcurrency;
    final String threadPool;
    final Iterable<Listener> listeners;

    private XmlListenersConfig(final ListenersType type, final ListenersType... others) {
      this.dispatcherConcurrency = type.getDispatcherConcurrency().intValue();
      String threadPool = type.getDispatcherThreadPool();
      Set<Listener> listenerSet = new HashSet<Listener>();
      final List<ListenersType.Listener> xmlListeners = type.getListener();
      extractListeners(listenerSet, xmlListeners);

      for (ListenersType other : others) {
        if (threadPool == null && other.getDispatcherThreadPool() != null) {
          threadPool = other.getDispatcherThreadPool();
        }
        extractListeners(listenerSet, other.getListener());
      }

      this.threadPool = threadPool;
      this.listeners = !listenerSet.isEmpty() ? listenerSet : null;
    }

    private void extractListeners(Set<Listener> listenerSet, List<ListenersType.Listener> xmlListeners) {
      if(xmlListeners != null) {
        for(final ListenersType.Listener listener : xmlListeners) {
          listenerSet.add(new Listener() {
            @Override
            public String className() {
              return listener.getClazz();
            }

            @Override
            public EventFiringType eventFiring() {
              return listener.getEventFiringMode();
            }

            @Override
            public EventOrderingType eventOrdering() {
              return listener.getEventOrderingMode();
            }

            @Override
            public List<EventType> fireOn() {
              return listener.getEventsToFireOn();
            }
          });
        }
      }
    }

    @Override
    public int dispatcherConcurrency() {
      return dispatcherConcurrency;
    }

    @Override
    public String threadPool() {
      return threadPool;
    }

    @Override
    public Iterable<Listener> listeners() {
      return listeners;
    }

  }

  private static class XmlExpiry implements Expiry {

    final ExpiryType type;

    private XmlExpiry(final ExpiryType type) {
      this.type = type;
    }

    @Override
    public boolean isUserDef() {
      return type != null && type.getClazz() != null;
    }

    @Override
    public boolean isTTI() {
      return type != null && type.getTti() != null;
    }

    @Override
    public boolean isTTL() {
      return type != null && type.getTtl() != null;
    }

    @Override
    public String type() {
      return type.getClazz();
    }

    @Override
    public long value() {
      final TimeType time;
      if(isTTI()) {
        time = type.getTti();
      } else {
        time = type.getTtl();
      }
      return time == null ? 0L : time.getValue().longValue();
    }

    @Override
    public TimeUnit unit() {
      final TimeType time;
      if(isTTI()) {
        time = type.getTti();
      } else {
        time = type.getTtl();
      }
      if(time != null) {
        return XmlModel.convertToJavaTimeUnit(time.getUnit());
      }
      return null;
    }
  }

  private static class XmlSizeOfEngineLimits implements SizeOfEngineLimits {

    private final SizeofType sizeoflimits;

    private XmlSizeOfEngineLimits(SizeofType sizeoflimits) {
      this.sizeoflimits = sizeoflimits;
    }

    @Override
    public long getMaxObjectGraphSize() {
      SizeofType.MaxObjectGraphSize value = sizeoflimits.getMaxObjectGraphSize();
      if (value == null) {
        return new BigInteger(JaxbHelper.findDefaultValue(sizeoflimits, "maxObjectGraphSize")).longValue();
      } else {
        return value.getValue().longValue();
      }
    }

    @Override
    public long getMaxObjectSize() {
      MemoryType value = sizeoflimits.getMaxObjectSize();
      if (value == null) {
        return new BigInteger(JaxbHelper.findDefaultValue(sizeoflimits, "maxObjectSize")).longValue();
      } else {
        return value.getValue().longValue();
      }
    }

    @Override
    public MemoryUnit getUnit() {
      MemoryType value = sizeoflimits.getMaxObjectSize();
      if (value == null) {
        return MemoryUnit.valueOf(new ObjectFactory().createMemoryType().getUnit().value().toUpperCase());
      } else {
        return MemoryUnit.valueOf(value.getUnit().value().toUpperCase());
      }
    }

  }

  private static class XmlWriteBehind implements WriteBehind {

    private final CacheLoaderWriterType.WriteBehind writebehind;

    private XmlWriteBehind(CacheLoaderWriterType.WriteBehind writebehind) {
      this.writebehind = writebehind;
    }

    @Override
    public int maxQueueSize() {
      return this.writebehind.getSize().intValue();
    }

    @Override
    public int concurrency() {
      return this.writebehind.getConcurrency().intValue() ;
    }

    @Override
    public String threadPool() {
      return this.writebehind.getThreadPool();
    }

    @Override
    public Batching batching() {
      CacheLoaderWriterType.WriteBehind.Batching batching = writebehind.getBatching();
      if (batching == null) {
        return null;
      } else {
        return new XmlBatching(batching);
      }
    }

  }

  private static class XmlBatching implements Batching {

    private final CacheLoaderWriterType.WriteBehind.Batching batching;

    private XmlBatching(CacheLoaderWriterType.WriteBehind.Batching batching) {
      this.batching = batching;
    }

    @Override
    public boolean isCoalesced() {
      return this.batching.isCoalesce();
    }

    @Override
    public int batchSize() {
      return this.batching.getBatchSize().intValue();
    }

    @Override
    public long maxDelay() {
      return this.batching.getMaxWriteDelay().getValue().longValue();
    }

    @Override
    public TimeUnit maxDelayUnit() {
      return XmlModel.convertToJavaTimeUnit(this.batching.getMaxWriteDelay().getUnit());
    }

  }

  private static class XmlDiskStoreSettings implements DiskStoreSettings {

    private final DiskStoreSettingsType diskStoreSettings;

    private XmlDiskStoreSettings(DiskStoreSettingsType diskStoreSettings) {
      this.diskStoreSettings = diskStoreSettings;
    }

    @Override
    public int writerConcurrency() {
      return this.diskStoreSettings.getWriterConcurrency().intValue();
    }

    @Override
    public String threadPool() {
      return this.diskStoreSettings.getThreadPool();
    }

  }

}
