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
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.BaseCacheType;
import org.ehcache.xml.model.CacheDefinition;
import org.ehcache.xml.model.CacheEntryType;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.CacheTemplateType;
import org.ehcache.xml.model.CacheType;
import org.ehcache.xml.model.ConfigType;
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
import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Source;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedAction;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static java.security.AccessController.doPrivileged;
import static java.util.Arrays.asList;
import static java.util.Spliterators.spliterator;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.collectingAndThen;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ConfigurationBuilder.newConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.util.ClassLoading.servicesOfType;
import static org.ehcache.xml.XmlConfiguration.CORE_SCHEMA_URL;
import static org.ehcache.xml.XmlConfiguration.getClassForName;

/**
 * Provides support for parsing a cache configuration expressed in XML.
 */
public class ConfigurationParser {

  private static final Pattern SYSPROP = Pattern.compile("\\$\\{([^}]+)\\}");
  private static final SchemaFactory XSD_SCHEMA_FACTORY = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
  private static Schema newSchema(Source... schemas) throws SAXException {
    synchronized (XSD_SCHEMA_FACTORY) {
      return XSD_SCHEMA_FACTORY.newSchema(schemas);
    }
  }
  private static final TransformerFactory TRANSFORMER_FACTORY = TransformerFactory.newInstance();

  private static final QName CORE_SCHEMA_ROOT_NAME;
  static {
    ObjectFactory objectFactory = new ObjectFactory();
    CORE_SCHEMA_ROOT_NAME = objectFactory.createConfig(objectFactory.createConfigType()).getName();
  }

  static final CoreCacheConfigurationParser CORE_CACHE_CONFIGURATION_PARSER = new CoreCacheConfigurationParser();

  private final Schema schema;
  private final JAXBContext jaxbContext = JAXBContext.newInstance(ConfigType.class);
  private final DocumentBuilder documentBuilder;

  private final ServiceCreationConfigurationParser serviceCreationConfigurationParser;
  private final ServiceConfigurationParser serviceConfigurationParser;
  private final ResourceConfigurationParser resourceConfigurationParser;

  static String replaceProperties(String originalValue) {
    Matcher matcher = SYSPROP.matcher(originalValue);

    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      final String property = matcher.group(1);
      final String value = doPrivileged((PrivilegedAction<String>) () -> System.getProperty(property));
      if (value == null) {
        throw new IllegalStateException(String.format("Replacement for ${%s} not found!", property));
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(sb);
    final String resolvedValue = sb.toString();
    return resolvedValue.equals(originalValue) ? null : resolvedValue;
  }

  @SuppressWarnings("unchecked")
  private static <T> Stream<T> stream(Iterable<? super T> iterable) {
    return StreamSupport.stream(spliterator((Iterator<T>) iterable.iterator(), Long.MAX_VALUE, 0), false);
  }

  ConfigurationParser() throws IOException, SAXException, JAXBException, ParserConfigurationException {
    serviceCreationConfigurationParser = ConfigurationParser.<CacheManagerServiceConfigurationParser<?>>stream(
      servicesOfType(CacheManagerServiceConfigurationParser.class))
      .collect(collectingAndThen(toMap(CacheManagerServiceConfigurationParser::getServiceType, identity(),
        (a, b) -> a.getClass().isInstance(b) ? b : a), ServiceCreationConfigurationParser::new));

    serviceConfigurationParser = ConfigurationParser.<CacheServiceConfigurationParser<?>>stream(
      servicesOfType(CacheServiceConfigurationParser.class))
      .collect(collectingAndThen(toMap(CacheServiceConfigurationParser::getServiceType, identity(),
        (a, b) -> a.getClass().isInstance(b) ? b : a), ServiceConfigurationParser::new));

    resourceConfigurationParser = stream(servicesOfType(CacheResourceConfigurationParser.class))
      .flatMap(p -> p.getResourceTypes().stream().map(t -> new AbstractMap.SimpleImmutableEntry<>(t, p)))
      .collect(collectingAndThen(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a.getClass().isInstance(b) ? b : a),
        m -> new ResourceConfigurationParser(new HashSet<>(m.values()))));

    schema = discoverSchema(new StreamSource(CORE_SCHEMA_URL.openStream()));
    documentBuilder = documentBuilder(schema);
  }

  <K, V> CacheConfigurationBuilder<K, V> parseServiceConfigurations(CacheConfigurationBuilder<K, V> cacheBuilder,
                                                                    ClassLoader cacheClassLoader, CacheTemplate cacheDefinition)
    throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    cacheBuilder = CORE_CACHE_CONFIGURATION_PARSER.parseConfiguration(cacheDefinition, cacheClassLoader, cacheBuilder);
    return serviceConfigurationParser.parseConfiguration(cacheDefinition, cacheClassLoader, cacheBuilder);
  }

  private static void substituteSystemProperties(Node node) {
    Stack<NodeList> nodeLists = new Stack<>();
    nodeLists.push(node.getChildNodes());
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
            final String newValue = replaceProperties(attributeNode.getNodeValue());
            if (newValue != null) {
              attributeNode.setNodeValue(newValue);
            }
          }
        }
        if (currentNode.getNodeType() == Node.TEXT_NODE) {
          final String newValue = replaceProperties(currentNode.getNodeValue());
          if (newValue != null) {
            currentNode.setNodeValue(newValue);
          }
        }
      }
    }
  }

  private static Iterable<CacheDefinition> getCacheElements(ConfigType configType) {
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

  private Map<String, XmlConfiguration.Template> getTemplates(ConfigType configType) {
    final Map<String, XmlConfiguration.Template> templates = new HashMap<>();
    final List<BaseCacheType> cacheOrCacheTemplate = configType.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if (baseCacheType instanceof CacheTemplateType) {
        final CacheTemplate cacheTemplate = new CacheTemplate.Impl(((CacheTemplateType) baseCacheType));
        templates.put(cacheTemplate.id(), parseTemplate(cacheTemplate));
      }
    }
    return Collections.unmodifiableMap(templates);
  }

  private XmlConfiguration.Template parseTemplate(CacheTemplate template) {
    return new XmlConfiguration.Template() {
      @Override
      public <K, V> CacheConfigurationBuilder<K, V> builderFor(ClassLoader classLoader, Class<K> keyType, Class<V> valueType, ResourcePools resources) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        checkTemplateTypeConsistency("key", classLoader, keyType, template);
        checkTemplateTypeConsistency("value", classLoader, valueType, template);

        if ((resources == null || resources.getResourceTypeSet().isEmpty()) && template.getHeap() == null && template.getResources().isEmpty()) {
          throw new IllegalStateException("Template defines no resources, and none were provided");
        }

        if (resources == null) {
          resources = resourceConfigurationParser.parseResourceConfiguration(template, newResourcePoolsBuilder());
        }

        return parseServiceConfigurations(newCacheConfigurationBuilder(keyType, valueType, resources), classLoader, template);
      }
    };
  }

  private static <T> void checkTemplateTypeConsistency(String type, ClassLoader classLoader, Class<T> providedType, CacheTemplate template) throws ClassNotFoundException {
    Class<?> templateType;
    if (type.equals("key")) {
      templateType = getClassForName(template.keyType(), classLoader);
    } else {
      templateType = getClassForName(template.valueType(), classLoader);
    }

    if(providedType == null || !templateType.isAssignableFrom(providedType)) {
      throw new IllegalArgumentException("CacheTemplate '" + template.id() + "' declares " + type + " type of " + templateType.getName() + ". Provided: " + providedType);
    }
  }

  public Document uriToDocument(URI uri) throws IOException, SAXException {
    return documentBuilder.parse(uri.toString());
  }

  public XmlConfigurationWrapper documentToConfig(Document document, ClassLoader classLoader, Map<String, ClassLoader> cacheClassLoaders) throws JAXBException, ClassNotFoundException, InstantiationException, IllegalAccessException {
    substituteSystemProperties(document);

    Element root = document.getDocumentElement();

    QName rootName = new QName(root.getNamespaceURI(), root.getLocalName());
    if (!CORE_SCHEMA_ROOT_NAME.equals(rootName)) {
      throw new XmlConfigurationException("Expecting " + CORE_SCHEMA_ROOT_NAME + " element; found " + rootName);
    }

    Class<ConfigType> configTypeClass = ConfigType.class;
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    ConfigType jaxbModel = unmarshaller.unmarshal(document, configTypeClass).getValue();

    ConfigurationBuilder managerBuilder = newConfigurationBuilder().withClassLoader(classLoader);
    managerBuilder = serviceCreationConfigurationParser.parseServiceCreationConfiguration(jaxbModel, classLoader, managerBuilder);

    for (CacheDefinition cacheDefinition : getCacheElements(jaxbModel)) {
      String alias = cacheDefinition.id();
      if(managerBuilder.containsCache(alias)) {
        throw new XmlConfigurationException("Two caches defined with the same alias: " + alias);
      }

      ClassLoader cacheClassLoader = cacheClassLoaders.get(alias);
      boolean classLoaderConfigured = cacheClassLoader != null;

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

    Map<String, XmlConfiguration.Template> templates = getTemplates(jaxbModel);

    return new XmlConfigurationWrapper(managerBuilder.build(), templates);
  }

  public Document configToDocument(Configuration configuration) throws JAXBException {
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

    JAXBElement<ConfigType> root = new ObjectFactory().createConfig(configType);

    Marshaller marshaller = jaxbContext.createMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    marshaller.setSchema(schema);

    Document document = documentBuilder.newDocument();
    marshaller.marshal(root, document);
    return document;
  }

  public static class FatalErrorHandler implements ErrorHandler {

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
    private final Map<String, XmlConfiguration.Template> templates;

    public XmlConfigurationWrapper(Configuration configuration, Map<String, XmlConfiguration.Template> templates) {
      this.configuration = configuration;
      this.templates = templates;
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    public Map<String, XmlConfiguration.Template> getTemplates() {
      return templates;
    }
  }

  public static String documentToText(Document xml) throws IOException, TransformerException {
    try (StringWriter writer = new StringWriter()) {
      TRANSFORMER_FACTORY.newTransformer().transform(new DOMSource(xml), new StreamResult(writer));
      return writer.toString();
    }
  }

  public static String urlToText(URL url, String encoding) throws IOException {
    Charset charset = encoding == null ? StandardCharsets.UTF_8 : Charset.forName(encoding);
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), charset))) {
      return reader.lines().collect(joining(System.lineSeparator()));
    }
  }

  public static DocumentBuilder documentBuilder(Schema schema) throws ParserConfigurationException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    factory.setNamespaceAware(true);
    factory.setIgnoringComments(true);
    factory.setIgnoringElementContentWhitespace(true);
    factory.setSchema(schema);
    DocumentBuilder documentBuilder = factory.newDocumentBuilder();
    documentBuilder.setErrorHandler(new FatalErrorHandler());
    return documentBuilder;
  }

  public static Schema discoverSchema(Source ... fixedSources) throws SAXException, IOException {
    ArrayList<Source> schemaSources = new ArrayList<>(asList(fixedSources));
    for (CacheManagerServiceConfigurationParser<?> p : servicesOfType(CacheManagerServiceConfigurationParser.class)) {
      schemaSources.add(p.getXmlSchema());
    }
    for (CacheServiceConfigurationParser<?> p : servicesOfType(CacheServiceConfigurationParser.class)) {
      schemaSources.add(p.getXmlSchema());
    }
    for (CacheResourceConfigurationParser p : servicesOfType(CacheResourceConfigurationParser.class)) {
      schemaSources.add(p.getXmlSchema());
    }
    return newSchema(schemaSources.toArray(new Source[0]));
  }
}
