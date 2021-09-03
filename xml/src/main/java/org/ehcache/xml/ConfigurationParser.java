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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.BaseCacheType;
import org.ehcache.xml.model.CacheDefinition;
import org.ehcache.xml.model.CacheEntryType;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheTemplateType;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.xml.model.ObjectFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import java.io.IOException;
import java.io.StringWriter;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ConfigurationBuilder.newConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.xml.XmlConfiguration.getClassForName;

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
  static final String CORE_SCHEMA_NAMESPACE = "http://www.ehcache.org/v3";
  private static final String CORE_SCHEMA_ROOT_ELEMENT = "config";
  static final String CORE_SCHEMA_JAXB_MODEL_PACKAGE = ConfigType.class.getPackage().getName();

  static final CoreCacheConfigurationParser CORE_CACHE_CONFIGURATION_PARSER = new CoreCacheConfigurationParser();

  private final Schema schema;
  private final JAXBContext jaxbContext = JAXBContext.newInstance(CORE_SCHEMA_JAXB_MODEL_PACKAGE, ConfigType.class.getClassLoader());

  private final ServiceCreationConfigurationParser serviceCreationConfigurationParser;
  private final ServiceConfigurationParser serviceConfigurationParser;
  private final ResourceConfigurationParser resourceConfigurationParser;

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

  private static <T> T retrieveChild(T val1, T val2) {
    if (val1 == null) {
      return val2;
    }
    if (val1.getClass().isInstance(val2)) {
      return val2;
    } else {
      return val1;
    }
  }

  public ConfigurationParser() throws IOException, SAXException, JAXBException, ParserConfigurationException {
    Collection<Source> schemaSources = new ArrayList<>();
    schemaSources.add(new StreamSource(CORE_SCHEMA_URL.openStream()));

    Map<Class<?>, CacheManagerServiceConfigurationParser<?>> xmlParserMap = new HashMap<>();
    for (CacheManagerServiceConfigurationParser<?> parser : ClassLoading.libraryServiceLoaderFor(CacheManagerServiceConfigurationParser.class)) {
      xmlParserMap.compute(parser.getServiceType(), (k, parserVal) -> retrieveChild(parserVal, parser));
    }
    for (CacheManagerServiceConfigurationParser<?> parser : xmlParserMap.values()) {
      schemaSources.add(parser.getXmlSchema());
    }
    serviceCreationConfigurationParser = new ServiceCreationConfigurationParser(xmlParserMap);

    Map<Class<?>, CacheServiceConfigurationParser<?>> cacheXmlParserMap = new HashMap<>();
    for (CacheServiceConfigurationParser<?> parser : ClassLoading.libraryServiceLoaderFor(CacheServiceConfigurationParser.class)) {
      cacheXmlParserMap.compute(parser.getServiceType(), (k, parserVal) -> retrieveChild(parserVal, parser));
    }
    for (CacheServiceConfigurationParser<?> parser : cacheXmlParserMap.values()) {
      schemaSources.add(parser.getXmlSchema());
    }
    serviceConfigurationParser = new ServiceConfigurationParser(cacheXmlParserMap);

    // Parsers for /config/cache/resources extensions
    Map<Class<?>, CacheResourceConfigurationParser> resourceXmlParserMap = new HashMap<>();
    for (CacheResourceConfigurationParser parser : ClassLoading.libraryServiceLoaderFor(CacheResourceConfigurationParser.class)) {
      Set<Class<? extends ResourcePool>> resourcePoolSet = parser.getResourceTypes();
      for (Class<? extends ResourcePool> x : resourcePoolSet) {
        resourceXmlParserMap.compute(x, (k, parserVal) -> retrieveChild(parserVal, parser));
      }
    }
    Set<CacheResourceConfigurationParser> resourceXmlParsers = new HashSet<>();
    for (CacheResourceConfigurationParser parser : resourceXmlParserMap.values()) {
      if (!resourceXmlParsers.contains(parser)) {
        resourceXmlParsers.add(parser);
        schemaSources.add(parser.getXmlSchema());
      }
    }

    this.schema = newSchema(schemaSources.toArray(new Source[schemaSources.size()]));
    resourceConfigurationParser = new ResourceConfigurationParser(this.schema, resourceXmlParsers);
  }

  ResourceConfigurationParser getResourceConfigurationParser() {
    return resourceConfigurationParser;
  }

  public XmlConfigurationWrapper parseConfiguration(String uri, ClassLoader classLoader, Map<String, ClassLoader> cacheClassLoaders)
    throws IOException, SAXException, JAXBException, ParserConfigurationException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    ConfigType configType = parseXml(uri);

    ConfigurationBuilder managerBuilder = newConfigurationBuilder().withClassLoader(classLoader);
    managerBuilder = serviceCreationConfigurationParser.parseServiceCreationConfiguration(configType, classLoader, managerBuilder);

    for (CacheDefinition cacheDefinition : getCacheElements(configType)) {
      String alias = cacheDefinition.id();
      if(managerBuilder.containsCache(alias)) {
        throw new XmlConfigurationException("Two caches defined with the same alias: " + alias);
      }

      ClassLoader cacheClassLoader = cacheClassLoaders.get(alias);
      boolean classLoaderConfigured = false;
      if (cacheClassLoader != null) {
        classLoaderConfigured = true;
      }

      if (cacheClassLoader == null) {
        if (classLoader != null) {
          cacheClassLoader = classLoader;
        } else {
          cacheClassLoader = ClassLoading.getDefaultClassLoader();
        }
      }

      Class<?> keyType = getClassForName(cacheDefinition.keyType(), cacheClassLoader);
      Class<?> valueType = getClassForName(cacheDefinition.valueType(), cacheClassLoader);

      ResourcePools resourcePools = resourceConfigurationParser.parseResourceConfiguration(cacheDefinition, newResourcePoolsBuilder());

      CacheConfigurationBuilder<?, ?> cacheBuilder = newCacheConfigurationBuilder(keyType, valueType, resourcePools);
      if (classLoaderConfigured) {
        cacheBuilder = cacheBuilder.withClassLoader(cacheClassLoader);
      }

      cacheBuilder = parseServiceConfigurations(cacheBuilder, cacheClassLoader, cacheDefinition);
      managerBuilder = managerBuilder.addCache(alias, cacheBuilder.build());
    }

    Map<String, CacheTemplate> templates = getTemplates(configType);

    return new XmlConfigurationWrapper(managerBuilder.build(), templates);
  }

  <K, V> CacheConfigurationBuilder<K, V> parseServiceConfigurations(CacheConfigurationBuilder<K, V> cacheBuilder,
                                                                    ClassLoader cacheClassLoader, CacheTemplate cacheDefinition)
    throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    cacheBuilder = CORE_CACHE_CONFIGURATION_PARSER.parseConfiguration(cacheDefinition, cacheClassLoader, cacheBuilder);
    return serviceConfigurationParser.parseConfiguration(cacheDefinition, cacheClassLoader, cacheBuilder);
  }

  public ConfigType parseXml(String uri) throws ParserConfigurationException, IOException, SAXException, JAXBException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);
    factory.setSchema(schema);

    DocumentBuilder domBuilder = factory.newDocumentBuilder();
    domBuilder.setErrorHandler(new FatalErrorHandler());
    Document document = domBuilder.parse(uri);
    Element dom = document.getDocumentElement();

    substituteSystemProperties(dom);

    if (!CORE_SCHEMA_ROOT_ELEMENT.equals(dom.getLocalName()) || !CORE_SCHEMA_NAMESPACE.equals(dom.getNamespaceURI())) {
      throw new XmlConfigurationException("Expecting {" + CORE_SCHEMA_NAMESPACE + "}" + CORE_SCHEMA_ROOT_ELEMENT
                                          + " element; found {" + dom.getNamespaceURI() + "}" + dom.getLocalName());
    }

    Class<ConfigType> configTypeClass = ConfigType.class;
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    return unmarshaller.unmarshal(dom, configTypeClass).getValue();
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
  public static Iterable<CacheDefinition> getCacheElements(ConfigType configType) {
    List<CacheDefinition> cacheCfgs = new ArrayList<>();
    final List<BaseCacheType> cacheOrCacheTemplate = configType.getCacheOrCacheTemplate();
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

        cacheCfgs.add(new CacheDefinition(cacheType.getAlias(), sources));
      }
    }

    return Collections.unmodifiableList(cacheCfgs);
  }

  public static Map<String, CacheTemplate> getTemplates(ConfigType configType) {
    final Map<String, CacheTemplate> templates = new HashMap<>();
    final List<BaseCacheType> cacheOrCacheTemplate = configType.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if (baseCacheType instanceof CacheTemplateType) {
        final CacheTemplateType cacheTemplate = (CacheTemplateType)baseCacheType;
        templates.put(cacheTemplate.getName(), new CacheTemplate.Impl(cacheTemplate.getName(), cacheTemplate));
      }
    }
    return Collections.unmodifiableMap(templates);
  }

  public String unparseConfiguration(Configuration configuration) throws JAXBException {
    ConfigType configType = new ConfigType();

    serviceCreationConfigurationParser.unparseServiceCreationConfiguration(configuration, configType);

    for (Map.Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : configuration.getCacheConfigurations().entrySet()) {
      CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurationEntry.getValue();

      CacheType cacheType = new CacheType().withAlias(cacheConfigurationEntry.getKey())
        .withKeyType(new CacheEntryType().withValue(cacheConfiguration.getKeyType().getName()))
        .withValueType(new CacheEntryType().withValue(cacheConfiguration.getValueType().getName()));

      resourceConfigurationParser.unparseResourceConfiguration(cacheConfiguration.getResourcePools(), cacheType);

      CORE_CACHE_CONFIGURATION_PARSER.unparseConfiguration(cacheConfiguration, cacheType);
      serviceConfigurationParser.unparseServiceConfiguration(cacheConfiguration, cacheType);
      configType.withCacheOrCacheTemplate(cacheType);
    }

    StringWriter writer = new StringWriter();
    JAXBElement<ConfigType> root = new ObjectFactory().createConfig(configType);

    Marshaller marshaller = jaxbContext.createMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    marshaller.setSchema(schema);

    marshaller.marshal(root, writer);
    return writer.toString();
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

  public static class XmlConfigurationWrapper {
    private final Configuration configuration;
    private final Map<String, CacheTemplate> templates;

    public XmlConfigurationWrapper(Configuration configuration, Map<String, CacheTemplate> templates) {
      this.configuration = configuration;
      this.templates = templates;
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    public Map<String, CacheTemplate> getTemplates() {
      return templates;
    }
  }

}
