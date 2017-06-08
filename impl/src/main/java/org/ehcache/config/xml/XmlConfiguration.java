/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */
package org.ehcache.config.xml;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.Configuration;
import org.ehcache.config.ConfigurationBuilder;
import org.ehcache.spi.CacheProvider;
import org.ehcache.spi.ServiceConfiguration;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

/**
 *
 * @author cdennis
 */
public class XmlConfiguration {

  private static final String CORE_NAMESPACE_URI = "http://www.ehcache.org/v3";
  
  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");
  private static final SchemaFactory XSD_SCHEMA_FACTORY = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
  private static Schema newSchema(Source[] schemas) throws SAXException {
    synchronized (XSD_SCHEMA_FACTORY) {
      return XSD_SCHEMA_FACTORY.newSchema(schemas);
    }
  }

  private final DocumentBuilder domBuilder;
  private final Map<URI, XmlConfigurationParser> xmlParsers = new HashMap<>();
          
  public XmlConfiguration() throws IOException, SAXException {
    Collection<Source> schemaSources = new ArrayList<>();
    for (XmlConfigurationParser parser : ServiceLoader.load(XmlConfigurationParser.class)) {
      schemaSources.add(parser.getXmlSchema());
      xmlParsers.put(parser.getNamespace(), parser);
    }
    schemaSources.add(new StreamSource(CORE_SCHEMA_URL.openStream()));
    
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);
    factory.setSchema(newSchema(schemaSources.toArray(new Source[schemaSources.size()])));

    try {
      this.domBuilder = factory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new AssertionError(e);
    }
    domBuilder.setErrorHandler(new FatalErrorHandler());
  }
  
  public Configuration parseConfiguration(URL xml) throws ClassNotFoundException, IOException, SAXException {
    try {
      Document dom = domBuilder.parse(xml.toExternalForm());
      ConfigurationBuilder configBuilder = new ConfigurationBuilder();

      Element config = dom.getDocumentElement();

      NodeList serviceElements = config.getElementsByTagNameNS(CORE_NAMESPACE_URI, "service");
      for (int i = 0; i < serviceElements.getLength(); i++) {
        configBuilder.addService(parseExtension((Element) serviceElements.item(i)));
      }

      NodeList cacheElements = config.getElementsByTagNameNS(CORE_NAMESPACE_URI, "cache");
      for (int i = 0; i < cacheElements.getLength(); i++) {
        Element cacheElement = (Element) cacheElements.item(i);
        String alias = getAttribute(cacheElement, "alias");
        configBuilder.addCache(alias, parseCacheElement(cacheElement));
      }

      return configBuilder.build();
    } finally {
      domBuilder.reset();
    }
  }

  private CacheConfiguration<?, ?> parseCacheElement(Element element) throws ClassNotFoundException {
    CacheConfigurationBuilder builder = new CacheConfigurationBuilder();

    String provider = getAttribute(element, "provider");
    String key = getElementTextContent(element, "key-type");
    String value = getElementTextContent(element, "value-type");

    Class<? extends CacheProvider> providerType = (Class<? extends CacheProvider>) getClassForName(provider);
    Class<?> keyType = getClassForName(key);
    Class<?> valueType = getClassForName(value);
    
    for (ServiceConfiguration<?> serviceConfig : parseExtensions(element)) {
      builder.addServiceConfig(serviceConfig);
    }    
    return builder.buildConfig(providerType, keyType, valueType);
  }

  private ServiceConfiguration<?> parseExtension(Element parent) {
    Collection<ServiceConfiguration<?>> configs = parseExtensions(parent);
    if (configs.size() == 1) {
      return configs.iterator().next();
    } else {
      throw new AssertionError();
    }
  }
  
  private Collection<ServiceConfiguration<?>> parseExtensions(Element parent) {
    Collection<ServiceConfiguration<?>> configs = new ArrayList<>();
    for (Node child = parent.getFirstChild(); child != null; child = child.getNextSibling()) {
      if (Node.ELEMENT_NODE == child.getNodeType()) {
        String namespaceString = child.getNamespaceURI();
        if (!namespaceString.equals(CORE_NAMESPACE_URI)) {
          URI namespace = URI.create(namespaceString);
          configs.add(xmlParsers.get(namespace).parse((Element) child));
        }
      }
    }
    return configs;
  }
  
  private static String getAttribute(Element element, String name) {
    return element.getAttribute(name);
  }
  
  private static String getElementTextContent(Element element, String name) {
    NodeList matches = element.getElementsByTagNameNS(CORE_NAMESPACE_URI, name);
    if (matches.getLength() == 1) {
      return matches.item(0).getTextContent();
    } else {
      throw new AssertionError();
    }
  }
  
  private static Class<?> getClassForName(String name) throws ClassNotFoundException {
    ClassLoader tccl = Thread.currentThread().getContextClassLoader();
    if (tccl == null) {
      return Class.forName(name);
    } else {
      try {
        return Class.forName(name, true, tccl);
      } catch (ClassNotFoundException e) {
        return Class.forName(name);
      }
    }
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
