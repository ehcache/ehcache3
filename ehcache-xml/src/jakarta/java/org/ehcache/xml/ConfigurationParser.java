/*
 * Copyright Terracotta, Inc.
 * Copyright IBM Corp. 2024, 2025
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
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Marshaller;
import jakarta.xml.bind.Unmarshaller;
import jakarta.xml.bind.helpers.DefaultValidationEventHandler;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.config.FluentConfigurationBuilder;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.builders.CacheConfigurationBuilder;
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
import org.ehcache.xml.model.ResourcesType;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import javax.xml.namespace.QName;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.Validator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

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
import static org.ehcache.xml.XmlUtil.mergePartialOrderings;
import static org.ehcache.xml.XmlUtil.namespaceUniqueParsersOfType;
import static org.ehcache.xml.XmlUtil.newSchema;
import static org.ehcache.xml.XmlUtil.stampExternalConfigurations;

/**
 * Provides support for parsing a cache configuration expressed in XML.
 */
public class ConfigurationParser {

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

  @SuppressWarnings("unchecked")
  private static <T> Stream<T> stream(Iterable<? super T> iterable) {
    return StreamSupport.stream(spliterator((Iterator<T>) iterable.iterator(), Long.MAX_VALUE, 0), false);
  }

  ConfigurationParser() throws IOException, SAXException, JAXBException, ParserConfigurationException {
    serviceCreationConfigurationParser = ConfigurationParser.<CacheManagerServiceConfigurationParser<?, ?>>stream(
        namespaceUniqueParsersOfType(CacheManagerServiceConfigurationParser.class))
      .collect(collectingAndThen(toMap(CacheManagerServiceConfigurationParser::getServiceType, identity(),
        (a, b) -> a.getClass().isInstance(b) ? b : a), ServiceCreationConfigurationParser::new));

    serviceConfigurationParser = ConfigurationParser.<CacheServiceConfigurationParser<?, ?>>stream(
        namespaceUniqueParsersOfType(CacheServiceConfigurationParser.class))
      .collect(collectingAndThen(toMap(CacheServiceConfigurationParser::getServiceType, identity(),
        (a, b) -> a.getClass().isInstance(b) ? b : a), ServiceConfigurationParser::new));

    resourceConfigurationParser = stream(servicesOfType(CacheResourceConfigurationParser.class))
      .flatMap(p -> p.getResourceTypes().stream().map(t -> new AbstractMap.SimpleImmutableEntry<>(t, p)))
      .collect(collectingAndThen(toMap(Map.Entry::getKey, Map.Entry::getValue, (a, b) -> a.getClass().isInstance(b) ? b : a),
        m -> new ResourceConfigurationParser(new HashSet<>(m.values()))));

    schema = discoverSchema(new StreamSource(CORE_SCHEMA_URL.openStream()));
    documentBuilder = documentBuilder(schema);
  }

  <K, V> CacheConfigurationBuilder<K, V> parseServiceConfigurations(Document document, CacheConfigurationBuilder<K, V> cacheBuilder,
                                                                    ClassLoader cacheClassLoader, CacheTemplate cacheDefinition)
    throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    cacheBuilder = CORE_CACHE_CONFIGURATION_PARSER.parse(cacheDefinition, cacheClassLoader, cacheBuilder);
    return serviceConfigurationParser.parse(document, cacheDefinition, cacheClassLoader, cacheBuilder);
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

  private Map<String, XmlConfiguration.Template> getTemplates(Document document, ConfigType configType) {
    final Map<String, XmlConfiguration.Template> templates = new HashMap<>();
    final List<BaseCacheType> cacheOrCacheTemplate = configType.getCacheOrCacheTemplate();
    for (BaseCacheType baseCacheType : cacheOrCacheTemplate) {
      if (baseCacheType instanceof CacheTemplateType) {
        final CacheTemplate cacheTemplate = new CacheTemplate.Impl(((CacheTemplateType) baseCacheType));
        templates.put(cacheTemplate.id(), parseTemplate(document, cacheTemplate));
      }
    }
    return Collections.unmodifiableMap(templates);
  }

  private XmlConfiguration.Template parseTemplate(Document document, CacheTemplate template) {
    return new XmlConfiguration.Template() {
      @Override
      public <K, V> CacheConfigurationBuilder<K, V> builderFor(ClassLoader classLoader, Class<K> keyType, Class<V> valueType, ResourcePools resources) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        checkTemplateTypeConsistency("key", classLoader, keyType, template);
        checkTemplateTypeConsistency("value", classLoader, valueType, template);

        if ((resources == null || resources.getResourceTypeSet().isEmpty()) && template.getHeap() == null && template.getResources().isEmpty()) {
          throw new IllegalStateException("Template defines no resources, and none were provided");
        }

        if (resources == null) {
          resources = resourceConfigurationParser.parse(template, newResourcePoolsBuilder(), classLoader);
        }

        return parseServiceConfigurations(document, newCacheConfigurationBuilder(keyType, valueType, resources), classLoader, template);
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
    Document annotatedDocument = stampExternalConfigurations(copyAndValidate(document));
    Element root = annotatedDocument.getDocumentElement();

    QName rootName = new QName(root.getNamespaceURI(), root.getLocalName());
    if (!CORE_SCHEMA_ROOT_NAME.equals(rootName)) {
      throw new XmlConfigurationException("Expecting " + CORE_SCHEMA_ROOT_NAME + " element; found " + rootName);
    }

    Class<ConfigType> configTypeClass = ConfigType.class;
    Unmarshaller unmarshaller = jaxbContext.createUnmarshaller();
    unmarshaller.setEventHandler(new DefaultValidationEventHandler());
    ConfigType jaxbModel = unmarshaller.unmarshal(annotatedDocument, configTypeClass).getValue();

    FluentConfigurationBuilder<?> managerBuilder = newConfigurationBuilder().withClassLoader(classLoader);
    managerBuilder = serviceCreationConfigurationParser.parse(annotatedDocument, jaxbModel, classLoader, managerBuilder);
    ResourcePools sharedResourcePools = resourceConfigurationParser.parse(jaxbModel.getSharedResources(), classLoader);
    managerBuilder = managerBuilder.withSharedResources(sharedResourcePools);

    for (CacheDefinition cacheDefinition : getCacheElements(jaxbModel)) {
      String alias = cacheDefinition.id();
      if(managerBuilder.getCache(alias) != null) {
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

      ResourcePools resourcePools = resourceConfigurationParser.parse(cacheDefinition, newResourcePoolsBuilder(), classLoader);

      CacheConfigurationBuilder<?, ?> cacheBuilder = newCacheConfigurationBuilder(keyType, valueType, resourcePools);
      if (classLoaderConfigured) {
        cacheBuilder = cacheBuilder.withClassLoader(cacheClassLoader);
      }

      cacheBuilder = parseServiceConfigurations(annotatedDocument, cacheBuilder, cacheClassLoader, cacheDefinition);
      managerBuilder = managerBuilder.withCache(alias, cacheBuilder.build());
    }

    Map<String, XmlConfiguration.Template> templates = getTemplates(annotatedDocument, jaxbModel);

    return new XmlConfigurationWrapper(managerBuilder.build(), templates);
  }

  private Document copyAndValidate(Document document) {
    try {
      Validator validator = schema.newValidator();
      Document newDocument = documentBuilder.newDocument();
      newDocument.setStrictErrorChecking(false);
      validator.validate(new DOMSource(document), new DOMResult(newDocument));
      return newDocument;
    } catch (SAXException | IOException e) {
      throw new AssertionError(e);
    }
  }

  public Document configToDocument(Configuration configuration) throws JAXBException {
    ConfigType configType = new ConfigType();
    Document document = documentBuilder.newDocument();

    serviceCreationConfigurationParser.unparse(document, configuration, configType);
    ResourcesType sharedResources = resourceConfigurationParser.unparse(document, configuration.getSharedResourcePools());
    if (!sharedResources.getResource().isEmpty()) {
      configType.withSharedResources(sharedResources);
    }

    for (Map.Entry<String, CacheConfiguration<?, ?>> cacheConfigurationEntry : configuration.getCacheConfigurations().entrySet()) {
      CacheConfiguration<?, ?> cacheConfiguration = cacheConfigurationEntry.getValue();

      CacheType cacheType = new CacheType().withAlias(cacheConfigurationEntry.getKey())
        .withKeyType(new CacheEntryType().withValue(cacheConfiguration.getKeyType().getName()))
        .withValueType(new CacheEntryType().withValue(cacheConfiguration.getValueType().getName()))
        .withResources(resourceConfigurationParser.unparse(document, cacheConfiguration.getResourcePools()));
      cacheType = CORE_CACHE_CONFIGURATION_PARSER.unparse(cacheConfiguration, cacheType);
      cacheType = serviceConfigurationParser.unparse(document, cacheConfiguration, cacheType);
      configType = configType.withCacheOrCacheTemplate(cacheType);
    }

    JAXBElement<ConfigType> root = new ObjectFactory().createConfig(configType);

    Marshaller marshaller = jaxbContext.createMarshaller();
    marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
    marshaller.setSchema(schema);

    marshaller.marshal(root, document);
    return document;
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
      transformer().transform(new DOMSource(xml), new StreamResult(writer));
      return writer.toString();
    }
  }

  private static Transformer transformer() throws TransformerConfigurationException {
    Transformer transformer = TRANSFORMER_FACTORY.newTransformer();
    transformer.setOutputProperty(OutputKeys.METHOD, "xml");
    transformer.setOutputProperty(OutputKeys.ENCODING, StandardCharsets.UTF_8.name());
    transformer.setOutputProperty(OutputKeys.INDENT, "yes");
    transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "2");
    return transformer;
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
    documentBuilder.setErrorHandler(new XmlUtil.FatalErrorHandler());
    return documentBuilder;
  }

  public static Schema discoverSchema(Source ... fixedSources) throws SAXException {
    Collection<List<URI>> neededNamespaces = new ArrayList<>();
    Map<URI, Supplier<Source>> sources = new HashMap<>();

    for (@SuppressWarnings("rawtypes") Iterable<? extends Parser> parsers : asList(
      namespaceUniqueParsersOfType(CacheManagerServiceConfigurationParser.class),
      namespaceUniqueParsersOfType(CacheServiceConfigurationParser.class),
      /*
       * Resource parsers are allowed to share namespaces.
       */
      servicesOfType(CacheResourceConfigurationParser.class)
    )) {
      for (Parser<?> p : parsers) {
        List<URI> ordering = new ArrayList<>();
        for (Map.Entry<URI, Supplier<Source>> element : p.getSchema().entrySet()) {
          sources.putIfAbsent(element.getKey(), element.getValue());
          ordering.add(element.getKey());
        }
        neededNamespaces.add(ordering);
      }
    }

    List<URI> fullOrdering = mergePartialOrderings(neededNamespaces);

    List<Source> schemaSources = new ArrayList<>(asList(fixedSources));
    schemaSources.addAll(fullOrdering.stream().map(sources::get).map(Supplier::get).collect(Collectors.toList()));
    return newSchema(schemaSources.toArray(new Source[0]));
  }
}
