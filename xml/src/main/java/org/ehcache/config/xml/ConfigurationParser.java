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
import org.ehcache.config.xml.model.CacheType;
import org.ehcache.config.xml.model.ConfigType;
import org.ehcache.config.xml.model.ServiceType;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.util.ClassLoading;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
public class ConfigurationParser {

  private static final SchemaFactory XSD_SCHEMA_FACTORY = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);

  private final Map<URI, XmlConfigurationParser> xmlParsers = new HashMap<URI, XmlConfigurationParser>();
  private final ConfigType config;

  public ConfigurationParser(String xml, URL... sources) throws IOException, SAXException {
    Collection<Source> schemaSources = new ArrayList<Source>();
    for (XmlConfigurationParser parser : ClassLoading.libraryServiceLoaderFor(XmlConfigurationParser.class)) {
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

  public Iterable<ServiceConfiguration> getServiceConfigurations() {

    final ArrayList<ServiceConfiguration> serviceConfigurations = new ArrayList<ServiceConfiguration>();

    for (ServiceType serviceType : config.getService()) {
      final ServiceConfiguration<?> serviceConfiguration = parseExtension((Element)serviceType.getAny());
      serviceConfigurations.add(serviceConfiguration);
    }

    return Collections.unmodifiableList(serviceConfigurations);
  }

  public Iterable<CacheElement> getCacheElements() {
    List<CacheElement> cacheCfgs = new ArrayList<CacheElement>();
    final List<BaseCacheType> cacheOrCacheTemplate = config.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if(baseCacheType instanceof CacheType) {
        final CacheType cacheType = (CacheType)baseCacheType;
        cacheCfgs.add(new CacheElement() {
          @Override
          public String alias() {
            return cacheType.getAlias();
          }

          @Override
          public String keyType() {
            return cacheType.getKeyType();
          }

          @Override
          public String valueType() {
            return cacheType.getValueType();
          }

          @Override
          public Long capacityConstraint() {
            return cacheType.getCapacity() == null ? null : cacheType.getCapacity().longValue();
          }

          @Override
          public String evictionVeto() {
            return cacheType.getEvictionVeto();
          }

          @Override
          public String evictionPrioritizer() {
            return cacheType.getEvictionPrioritizer();
          }

          @Override
          public Iterable<ServiceConfiguration<?>> serviceConfigs() {
            Collection<ServiceConfiguration<?>> configs = new ArrayList<ServiceConfiguration<?>>();
            for (Object child : cacheType.getAny()) {
              configs.add(parseExtension((Element)child));
            }
            return configs;
          }
        });
      }
    }

    return Collections.unmodifiableList(cacheCfgs);
  }

  private ServiceConfiguration<?> parseExtension(final Element element) {
    URI namespace = URI.create(element.getNamespaceURI());
    final XmlConfigurationParser xmlConfigurationParser = xmlParsers.get(namespace);
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

  static interface CacheElement {

    String alias();

    String keyType();

    String valueType();
    
    Long capacityConstraint();

    String evictionVeto();
    
    String evictionPrioritizer();
    
    Iterable<ServiceConfiguration<?>> serviceConfigs();
  }

}
