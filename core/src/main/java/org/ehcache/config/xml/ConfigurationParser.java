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

import org.ehcache.spi.service.ServiceConfiguration;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
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
import java.util.ServiceLoader;

import javax.xml.XMLConstants;
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
  private static final String CORE_NAMESPACE_URI = "http://www.ehcache.org/v3";

  private final Map<URI, XmlConfigurationParser> xmlParsers = new HashMap<URI, XmlConfigurationParser>();
  private final Element config;

  public ConfigurationParser(String xml, URL... sources) throws IOException, SAXException {
    Collection<Source> schemaSources = new ArrayList<Source>();
    for (XmlConfigurationParser parser : ServiceLoader.load(XmlConfigurationParser.class)) {
      System.out.println(parser.getNamespace());
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
    config = domBuilder.parse(xml).getDocumentElement();
  }

  public Iterable<ServiceConfiguration> getServiceConfigurations() {
    final ArrayList<ServiceConfiguration> serviceConfigurations = new ArrayList<ServiceConfiguration>();
    NodeList serviceElements = config.getElementsByTagNameNS(CORE_NAMESPACE_URI, "service");
    for (int i = 0; i < serviceElements.getLength(); i++) {
      final Element item = (Element)serviceElements.item(i);
      for (int j = 0; j < item.getChildNodes().getLength(); j++) {
        final Element item1 = (Element)item.getChildNodes().item(j);
        final ServiceConfiguration<?> serviceConfiguration = parseExtension(item1);
        serviceConfigurations.add(serviceConfiguration);
      }
    }
    return Collections.unmodifiableList(serviceConfigurations);
  }

  public Iterable<CacheElement> getCacheElements() {
    List<CacheElement> cacheCfgs = new ArrayList<CacheElement>();
    NodeList cacheElements = config.getElementsByTagNameNS(CORE_NAMESPACE_URI, "cache");
    for (int i = 0; i < cacheElements.getLength(); i++) {
      final Element cacheElement = (Element) cacheElements.item(i);
      cacheCfgs.add(new CacheElement() {
        @Override
        public String alias() {
          return cacheElement.getAttribute("alias");
        }

        @Override
        public String keyType() {
          return getElementTextContent(cacheElement, "key-type");
        }

        @Override
        public String valueType() {
          return getElementTextContent(cacheElement, "value-type");
        }

        @Override
        public Iterable<ServiceConfiguration<?>> serviceConfigs() {
          Collection<ServiceConfiguration<?>> configs = new ArrayList<ServiceConfiguration<?>>();
          for (Node child = cacheElement.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (Node.ELEMENT_NODE == child.getNodeType()) {
              String namespaceString = child.getNamespaceURI();
              if (!namespaceString.equals(CORE_NAMESPACE_URI)) {
                configs.add(parseExtension((Element)child));
              }
            }
          }
          return configs;
        }
      });
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

    Iterable<ServiceConfiguration<?>> serviceConfigs();
  }

  private static String getElementTextContent(Element element, String name) {
    NodeList matches = element.getElementsByTagNameNS(CORE_NAMESPACE_URI, name);
    if (matches.getLength() == 1) {
      return matches.item(0).getTextContent();
    } else {
      throw new AssertionError();
    }
  }
}
