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

import org.ehcache.config.xml.model.BaseCacheType;
import org.ehcache.config.xml.model.CacheIntegration;
import org.ehcache.config.xml.model.CacheTemplateType;
import org.ehcache.config.xml.model.CacheType;
import org.ehcache.config.xml.model.ConfigType;
import org.ehcache.config.xml.model.ExpiryType;
import org.ehcache.config.xml.model.ServiceType;
import org.ehcache.config.xml.model.TimeType;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ClassLoading;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

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

/**
 * @author Alex Snaps
 */
class ConfigurationParser {

  private static final SchemaFactory XSD_SCHEMA_FACTORY = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

  private final Map<URI, XmlConfigurationParser<?>> xmlParsers = new HashMap<URI, XmlConfigurationParser<?>>();
  private final ConfigType config;

  public ConfigurationParser(String xml, URL... sources) throws IOException, SAXException {
    Collection<Source> schemaSources = new ArrayList<Source>();
    for (XmlConfigurationParser<?> parser : ClassLoading.libraryServiceLoaderFor(XmlConfigurationParser.class)) {
      schemaSources.add(parser.getXmlSchema());
      xmlParsers.put(parser.getNamespace(), parser);
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

  public Iterable<ServiceConfiguration<?>> getServiceConfigurations() {

    final ArrayList<ServiceConfiguration<?>> serviceConfigurations = new ArrayList<ServiceConfiguration<?>>();

    for (ServiceType serviceType : config.getService()) {
      final ServiceConfiguration<?> serviceConfiguration = parseExtension((Element)serviceType.getAny());
      serviceConfigurations.add(serviceConfiguration);
    }

    return Collections.unmodifiableList(serviceConfigurations);
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
              value = source.getKeyType();
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
          public String valueType() {
            String value = null;
            for (BaseCacheType source : sources) {
              value = source.getValueType();
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
          public Long capacityConstraint() {
            BigInteger value = null;
            for (BaseCacheType source : sources) {
              value = source.getCapacity();
              if (value != null) break;
            }
            return value != null ? value.longValue() : null;
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
            return new XmlExpiry(value);
          }

          @Override
          public boolean storeByValueOnHeap() {
            Boolean value = null;
            for (BaseCacheType source : sources) {
              value = source.isStoreByValueOnHeap();
              if (value != null) break;
            }
            return value == null ? false : value;
          }

          @Override
          public String loader() {
            String configClass = null;
            for (BaseCacheType source : sources) {
              final CacheIntegration integration = source.getIntegration();
              final CacheIntegration.Loader loader = integration != null ? integration.getLoader() : null;
              if(loader != null) {
                configClass = loader.getClazz();
                break;
              }
            }
            return configClass;
          }

          @Override
          public String writer() {
            String configClass = null;
            for (BaseCacheType source : sources) {
              final CacheIntegration integration = source.getIntegration();
              final CacheIntegration.Writer writer = integration != null ? integration.getWriter() : null;
              if(writer != null) {
                configClass = writer.getClazz();
                break;
              }
            }
            return configClass;
          }

          @Override
          public Iterable<ServiceConfiguration<?>> serviceConfigs() {
            Collection<ServiceConfiguration<?>> configs = new ArrayList<ServiceConfiguration<?>>();
            for (BaseCacheType source : sources) {
              for (Object child : source.getAny()) {
                configs.add(parseExtension((Element)child));
              }
            }
            return configs;
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
            String keyType = cacheTemplate.getKeyType();
            if (keyType == null) {
              keyType = JaxbHelper.findDefaultValue(cacheTemplate, "keyType");
            }
            return keyType;
          }

          @Override
          public String valueType() {
            String valueType = cacheTemplate.getValueType();
            if (valueType == null) {
              valueType = JaxbHelper.findDefaultValue(cacheTemplate, "valueType");
            }
            return valueType;
          }

          @Override
          public Long capacityConstraint() {
            final BigInteger capacity = cacheTemplate.getCapacity();
            return capacity == null ? null : capacity.longValue();
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
            return new XmlExpiry(cacheTemplate.getExpiry());
          }

          @Override
          public boolean storeByValueOnHeap() {
            final Boolean storeByValueOnHeap = cacheTemplate.isStoreByValueOnHeap();
            return storeByValueOnHeap == null ? false : storeByValueOnHeap;
          }

          @Override
          public String loader() {
            final CacheIntegration integration = cacheTemplate.getIntegration();
            final CacheIntegration.Loader loader = integration != null ? integration.getLoader() : null;
            return loader != null ? loader.getClazz() : null;
          }

          @Override
          public String writer() {
            final CacheIntegration integration = cacheTemplate.getIntegration();
            final CacheIntegration.Writer writer = integration != null ? integration.getWriter() : null;
            return writer != null ? writer.getClazz() : null;
          }

          @Override
          public Iterable<ServiceConfiguration<?>> serviceConfigs() {
            Collection<ServiceConfiguration<?>> configs = new ArrayList<ServiceConfiguration<?>>();
            for (Object child : cacheTemplate.getAny()) {
              configs.add(parseExtension((Element)child));
            }
            return configs;
          }
        });
      }
    }
    return Collections.unmodifiableMap(templates);
  }

  private ServiceConfiguration<?> parseExtension(final Element element) {
    URI namespace = URI.create(element.getNamespaceURI());
    final XmlConfigurationParser<?> xmlConfigurationParser = xmlParsers.get(namespace);
    if(xmlConfigurationParser == null) {
      throw new IllegalArgumentException("Can't find parser for namespace: " + namespace);
    }
    return xmlConfigurationParser.parse(element);
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

  static interface CacheTemplate {

    String keyType();

    String valueType();

    Long capacityConstraint();

    String evictionVeto();

    String evictionPrioritizer();

    Expiry expiry();

    boolean storeByValueOnHeap();

    String loader();

    String writer();

    Iterable<ServiceConfiguration<?>> serviceConfigs();

  }

  static interface CacheDefinition extends CacheTemplate {

    String id();

  }

  static interface Expiry {

    boolean isUserDef();

    boolean isTTI();

    boolean isTTL();

    String type();

    long value();

    TimeUnit unit();

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
}
