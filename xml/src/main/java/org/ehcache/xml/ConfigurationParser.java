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

import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.BaseCacheType;
import org.ehcache.xml.model.CacheDefinition;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheTemplateType;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.ConfigType;
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
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides support for parsing a cache configuration expressed in XML.
 */
public class ConfigurationParser {

  private static final Pattern SYSPROP = Pattern.compile("\\$\\{([^}]+)\\}");
  private static final SchemaFactory XSD_SCHEMA_FACTORY = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
  private static Schema newSchema(Source[] schemas) throws SAXException {
    synchronized (XSD_SCHEMA_FACTORY) {
      return XSD_SCHEMA_FACTORY.newSchema(schemas);
    }
  }

  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");
  public static final String CORE_SCHEMA_NAMESPACE = "http://www.ehcache.org/v3";
  private static final String CORE_SCHEMA_ROOT_ELEMENT = "config";
  private static final String CORE_SCHEMA_JAXB_MODEL_PACKAGE = ConfigType.class.getPackage().getName();

  private final Map<URI, CacheManagerServiceConfigurationParser<?>> xmlParsers = new HashMap<>();
  private final Map<URI, CacheServiceConfigurationParser<?>> cacheXmlParsers = new HashMap<>();
  private final Unmarshaller unmarshaller;
  private final Map<URI, CacheResourceConfigurationParser> resourceXmlParsers = new HashMap<>();
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
    Collection<Source> schemaSources = new ArrayList<>();
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

  public ConfigType getConfigRoot() {
    return this.config;
  }

  private void substituteSystemProperties(final Element dom) {
    final Properties properties = System.getProperties();
    Stack<NodeList> nodeLists = new Stack<>();
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
  public Iterable<CacheDefinition> getCacheElements() {
    List<CacheDefinition> cacheCfgs = new ArrayList<>();
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

        cacheCfgs.add(new CacheDefinition(cacheType.getAlias(), cacheXmlParsers, resourceXmlParsers, unmarshaller, sources));
      }
    }

    return Collections.unmodifiableList(cacheCfgs);
  }

  public Map<String, CacheTemplate> getTemplates() {
    final Map<String, CacheTemplate> templates = new HashMap<>();
    final List<BaseCacheType> cacheOrCacheTemplate = config.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if (baseCacheType instanceof CacheTemplateType) {
        final CacheTemplateType cacheTemplate = (CacheTemplateType)baseCacheType;
        templates.put(cacheTemplate.getName(), new CacheTemplate.Impl(cacheXmlParsers, resourceXmlParsers, unmarshaller, cacheTemplate));
      }
    }
    return Collections.unmodifiableMap(templates);
  }

  ServiceCreationConfiguration<?> parseExtension(final Element element) {
    URI namespace = URI.create(element.getNamespaceURI());
    final CacheManagerServiceConfigurationParser<?> cacheManagerServiceConfigurationParser = xmlParsers.get(namespace);
    if(cacheManagerServiceConfigurationParser == null) {
      throw new IllegalArgumentException("Can't find parser for namespace: " + namespace);
    }
    return cacheManagerServiceConfigurationParser.parseServiceCreationConfiguration(element);
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

}
