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

package org.ehcache.config.xml;

import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.config.xml.model.BaseCacheType;
import org.ehcache.config.xml.model.CacheIntegrationType;
import org.ehcache.config.xml.model.CacheTemplateType;
import org.ehcache.config.xml.model.CacheType;
import org.ehcache.config.xml.model.ConfigType;
import org.ehcache.config.xml.model.EventFiringType;
import org.ehcache.config.xml.model.EventOrderingType;
import org.ehcache.config.xml.model.EventType;
import org.ehcache.config.xml.model.ExpiryType;
import org.ehcache.config.xml.model.PersistableResourceType;
import org.ehcache.config.xml.model.ResourceType;
import org.ehcache.config.xml.model.ResourcesType;
import org.ehcache.config.xml.model.ServiceType;
import org.ehcache.config.xml.model.TimeType;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.util.ClassLoading;
import org.w3c.dom.Element;
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
import javax.xml.validation.SchemaFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * @author Alex Snaps
 */
class ConfigurationParser {

  private static final SchemaFactory XSD_SCHEMA_FACTORY = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

  private final Map<URI, CacheManagerServiceConfigurationParser<?>> xmlParsers = new HashMap<URI, CacheManagerServiceConfigurationParser<?>>();
  private final Map<URI, CacheServiceConfigurationParser<?>> cacheXmlParsers = new HashMap<URI, CacheServiceConfigurationParser<?>>();
  private final ConfigType config;

  public ConfigurationParser(String xml, URL... sources) throws IOException, SAXException {
    Collection<Source> schemaSources = new ArrayList<Source>();
    for (CacheManagerServiceConfigurationParser<?> parser : ClassLoading.libraryServiceLoaderFor(CacheManagerServiceConfigurationParser.class)) {
      schemaSources.add(parser.getXmlSchema());
      xmlParsers.put(parser.getNamespace(), parser);
    }
    for (CacheServiceConfigurationParser<?> parser : ClassLoading.libraryServiceLoaderFor(CacheServiceConfigurationParser.class)) {
      schemaSources.add(parser.getXmlSchema());
      cacheXmlParsers.put(parser.getNamespace(), parser);
    }
    for (URL source : sources) {
      schemaSources.add(new StreamSource(source.openStream()));
    }

    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);
    factory.setSchema(XSD_SCHEMA_FACTORY.newSchema(schemaSources.toArray(new Source[schemaSources.size()])));

    final DocumentBuilder domBuilder;
    try {
      domBuilder = factory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new AssertionError(e);
    }
    domBuilder.setErrorHandler(new FatalErrorHandler());
    final Element config = domBuilder.parse(xml).getDocumentElement();

    try {
      JAXBContext jc = JAXBContext.newInstance("org.ehcache.config.xml.model");
      Unmarshaller u = jc.createUnmarshaller();
      this.config = u.unmarshal(config, ConfigType.class).getValue();
    } catch (JAXBException e) {
      throw new RuntimeException(e);
    }

  }

  public Iterable<ServiceType> getServiceElements() {
    return config.getService();
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
          public String evictionVeto() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getEvictionVeto();
              if (value != null) break;
            }
            return value;
          }

          @Override
          public String evictionPrioritizer() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getEvictionPrioritizer();
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
              final CacheIntegrationType integration = source.getIntegration();
              final CacheIntegrationType.LoaderWriter loaderWriter = integration != null ? integration.getLoaderWriter() : null;
              if (loaderWriter != null) {
                configClass = loaderWriter.getClazz();
                break;
              }
            }
            return configClass;
          }

          @Override
          public Iterable<Listener> listeners() {
            Set<Listener> cacheListenerSet = new HashSet<Listener>();
            for (BaseCacheType source : sources) {
              final CacheIntegrationType integration = source.getIntegration();
              final List<CacheIntegrationType.Listener> listeners = integration != null ? integration.getListener() : null;
              if (listeners != null) {
                for (final CacheIntegrationType.Listener listener : listeners) {
                  cacheListenerSet.add(new Listener() {
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
                break;
              }
            }
            return !cacheListenerSet.isEmpty() ? cacheListenerSet : null;
          }


          @Override
          public Iterable<ServiceConfiguration<?>> serviceConfigs() {
            Collection<ServiceConfiguration<?>> configs = new ArrayList<ServiceConfiguration<?>>();
            for (BaseCacheType source : sources) {
              for (Object child : source.getAny()) {
                configs.add(parseCacheExtension((Element) child));
              }
            }
            return configs;
          }

          @Override
          public Iterable<ResourcePool> resourcePools() {
            Collection<ResourcePool> resourcePools = new ArrayList<ResourcePool>();
            for (BaseCacheType source : sources) {
              ResourceType directHeapResource = source.getHeap();
              if (directHeapResource != null) {
                resourcePools.add(new ResourcePoolImpl(org.ehcache.config.ResourceType.Core.HEAP, directHeapResource.getSize()
                    .longValue(), parseUnit(directHeapResource), false));
              } else {
                ResourcesType resources = source.getResources();
                if (resources != null) {
                  ResourceType heapResource = resources.getHeap();
                  if (heapResource != null) {
                    resourcePools.add(new ResourcePoolImpl(org.ehcache.config.ResourceType.Core.HEAP, heapResource.getSize()
                        .longValue(), parseUnit(heapResource), false));
                  }
                  ResourceType offheapResource = resources.getOffheap();
                  if (offheapResource != null) {
                    resourcePools.add(new ResourcePoolImpl(org.ehcache.config.ResourceType.Core.OFFHEAP, offheapResource
                        .getSize()
                        .longValue(), parseUnit(offheapResource), false));
                  }
                  PersistableResourceType diskResource = resources.getDisk();
                  if (diskResource != null) {
                    resourcePools.add(new ResourcePoolImpl(org.ehcache.config.ResourceType.Core.DISK, diskResource.getSize()
                        .longValue(), parseUnit(diskResource), diskResource.isPersistent()));
                  }
                }
              }
            }
            return resourcePools;
          }

          @Override
          public WriteBehind writeBehind() {
            for (BaseCacheType source : sources) {
              final CacheIntegrationType integration = source.getIntegration();
              final CacheIntegrationType.WriteBehind writebehind = integration != null ? integration.getWriteBehind() : null;
              if (writebehind != null) {
                return new XmlWriteBehind(writebehind);
              }
            }
            return null;
          }
        });
      }
    }

    return Collections.unmodifiableList(cacheCfgs);
  }

  private static final class ResourcePoolImpl implements ResourcePool {
    private final org.ehcache.config.ResourceType type;
    private final long size;
    private final ResourceUnit unit;
    private final boolean persistent;

    public ResourcePoolImpl(org.ehcache.config.ResourceType type, long size, ResourceUnit unit, boolean persistent) {
      this.type = type;
      this.size = size;
      this.unit = unit;
      this.persistent = persistent;
    }

    public org.ehcache.config.ResourceType getType() {
      return type;
    }

    public long getSize() {
      return size;
    }

    public ResourceUnit getUnit() {
      return unit;
    }

    @Override
    public boolean isPersistent() {
      return persistent;
    }
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
          public String evictionVeto() {
            return cacheTemplate.getEvictionVeto();
          }

          @Override
          public String evictionPrioritizer() {
            return cacheTemplate.getEvictionPrioritizer();
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
          public Iterable<Listener> listeners() {
            Set<Listener> listenerSet = new HashSet<Listener>();
            final CacheIntegrationType integration = cacheTemplate.getIntegration();
            final List<CacheIntegrationType.Listener> listeners = integration != null ? integration.getListener(): null;
            if(listeners != null) {
              for(final CacheIntegrationType.Listener listener : listeners) {
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
            return !listenerSet.isEmpty() ? listenerSet : null;
          }

          @Override
          public String loaderWriter() {
            final CacheIntegrationType integration = cacheTemplate.getIntegration();
            final CacheIntegrationType.LoaderWriter loaderWriter = integration != null ? integration.getLoaderWriter(): null;
            return loaderWriter != null ? loaderWriter.getClazz() : null;
          }

          @Override
          public Iterable<ServiceConfiguration<?>> serviceConfigs() {
            Collection<ServiceConfiguration<?>> configs = new ArrayList<ServiceConfiguration<?>>();
            for (Object child : cacheTemplate.getAny()) {
              configs.add((ServiceConfiguration<?>) parseExtension((Element)child));
              configs.add(parseCacheExtension((Element) child));
            }
            return configs;
          }

          @Override
          public Iterable<ResourcePool> resourcePools() {
            Collection<ResourcePool> resourcePools = new ArrayList<ResourcePool>();

            ResourceType directHeapResource = cacheTemplate.getHeap();
            if (directHeapResource != null) {
              resourcePools.add(new ResourcePoolImpl(org.ehcache.config.ResourceType.Core.HEAP, directHeapResource.getSize().longValue(), parseUnit(directHeapResource), false));
            } else {
              ResourcesType resources = cacheTemplate.getResources();
              if (resources != null) {
                ResourceType heapResource = resources.getHeap();
                if (heapResource != null) {
                  resourcePools.add(new ResourcePoolImpl(org.ehcache.config.ResourceType.Core.HEAP, heapResource.getSize().longValue(), parseUnit(heapResource), false));
                }
                ResourceType offheapResource = resources.getOffheap();
                if (offheapResource != null) {
                  resourcePools.add(new ResourcePoolImpl(org.ehcache.config.ResourceType.Core.OFFHEAP, offheapResource.getSize().longValue(), parseUnit(offheapResource), false));
                }
                PersistableResourceType diskResource = resources.getDisk();
                if (diskResource != null) {
                  resourcePools.add(new ResourcePoolImpl(org.ehcache.config.ResourceType.Core.DISK, diskResource.getSize().longValue(), parseUnit(diskResource), diskResource.isPersistent()));
                }
              }
            }

            return resourcePools;
          }

          @Override
          public WriteBehind writeBehind() {
            final CacheIntegrationType integration = cacheTemplate.getIntegration();
            final CacheIntegrationType.WriteBehind writebehind = integration != null ? integration.getWriteBehind(): null;
            return writebehind != null ? new XmlWriteBehind(writebehind) : null;
          }
        });
      }
    }
    return Collections.unmodifiableMap(templates);
  }

  private ResourceUnit parseUnit(ResourceType resourceType) {
    if (resourceType.getUnit().value().equalsIgnoreCase("entries")) {
      return EntryUnit.ENTRIES;
    } else {
      return MemoryUnit.valueOf(resourceType.getUnit().value().toUpperCase());
    }
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

    String evictionVeto();

    String evictionPrioritizer();

    Expiry expiry();

    String loaderWriter();

    Iterable<Listener> listeners();

    Iterable<ServiceConfiguration<?>> serviceConfigs();

    Iterable<ResourcePool> resourcePools();
    
    WriteBehind writeBehind();

  }

  interface CacheDefinition extends CacheTemplate {

    String id();

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
    
    boolean isCoalesced();
    
    int batchSize();
    
    int maxQueueSize();
    
    int concurrency();

    int minWriteDelay();
    
    int maxWriteDelay();
    
    int rateLimitPerSecond();
    
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
        switch (time.getUnit()) {
          case NANOS:
            return TimeUnit.NANOSECONDS;
          case MICROS:
          return TimeUnit.MICROSECONDS;
          case MILLIS:
            return TimeUnit.MILLISECONDS;
          case SECONDS:
            return TimeUnit.SECONDS;
          case MINUTES:
            return TimeUnit.MINUTES;
          case HOURS:
            return TimeUnit.HOURS;
          case DAYS:
            return TimeUnit.DAYS;
        }
      }
      return null;
    }
  }
  
  private static class XmlWriteBehind implements WriteBehind {
    
    private final CacheIntegrationType.WriteBehind writebehind;

    private XmlWriteBehind(CacheIntegrationType.WriteBehind writebehind) {
      this.writebehind = writebehind;
    }
    
    @Override
    public boolean isCoalesced() {
      return this.writebehind.isCoalesce();
    }

    @Override
    public int batchSize() {
      return this.writebehind.getBatchSize().intValue();
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
    public int minWriteDelay() {
      return this.writebehind.getMinWriteDelay().intValue();
    }

    @Override
    public int maxWriteDelay() {
      return this.writebehind.getMaxWriteDelay().intValue();
    }

    @Override
    public int rateLimitPerSecond() {
      return this.writebehind.getRateLimitPerSecond().intValue();
    }
    
  }
}
