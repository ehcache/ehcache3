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
import org.ehcache.config.Builder;
import org.ehcache.config.FluentConfigurationBuilder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.core.util.ClassLoading;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.w3c.dom.Document;

import java.lang.reflect.Array;
import java.net.URL;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static java.lang.Class.forName;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static org.ehcache.config.builders.ConfigurationBuilder.newConfigurationBuilder;
import static org.ehcache.xml.ConfigurationParser.documentToText;
import static org.ehcache.xml.XmlConfiguration.PrettyClassFormat.when;

/**
 * Exposes {@link org.ehcache.config.Configuration} and {@link CacheConfigurationBuilder} expressed
 * in a XML file that obeys the <a href="http://www.ehcache.org/v3" target="_blank">core Ehcache schema</a>.
 * <p>
 * Instances of this class are not thread-safe.
 */
public class XmlConfiguration implements Configuration {

  public static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");

  private final URL source;
  private final Document document;
  private final String renderedDocument;

  private final Configuration configuration;
  private final Map<String, ClassLoader> cacheClassLoaders;
  private final Map<String, Template> templates;

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url}.
   * <p>
   * The default ClassLoader will first try to use the thread context class loader, followed by the ClassLoader that
   * loaded the Ehcache classes.
   *
   * @param url URL pointing to the XML file's location
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(URL url)
      throws XmlConfigurationException {
    this(url, ClassLoading.getDefaultClassLoader());
  }

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url} and using the provided
   * {@code classLoader} to load user types (e.g. key and value Class instances).
   *
   * @param url URL pointing to the XML file's location
   * @param classLoader ClassLoader to use to load user types.
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(URL url, final ClassLoader classLoader)
      throws XmlConfigurationException {
    this(url, classLoader, Collections.emptyMap());
  }

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url} and using the provided
   * {@code classLoader} to load user types (e.g. key and value Class instances). The {@code cacheClassLoaders} will
   * let you specify a different {@link java.lang.ClassLoader} to use for each {@link org.ehcache.Cache} managed by
   * the {@link org.ehcache.CacheManager} configured using this {@link org.ehcache.xml.XmlConfiguration}. Caches with
   * aliases that do not appear in the map will use {@code classLoader} as a default.
   *
   * @param url URL pointing to the XML file's location
   * @param classLoader ClassLoader to use to load user types.
   * @param cacheClassLoaders the map with mappings between cache names and the corresponding class loaders
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(URL url, final ClassLoader classLoader, final Map<String, ClassLoader> cacheClassLoaders)
      throws XmlConfigurationException {

    this.source = requireNonNull(url, "The url can not be null");
    requireNonNull(classLoader, "The classLoader can not be null");
    this.cacheClassLoaders = requireNonNull(cacheClassLoaders, "The cacheClassLoaders map can not be null");

    try {
      ConfigurationParser parser = new ConfigurationParser();
      this.document = parser.uriToDocument(source.toURI());
      ConfigurationParser.XmlConfigurationWrapper configWrapper = parser.documentToConfig(document, classLoader, cacheClassLoaders);
      this.configuration = configWrapper.getConfiguration();
      this.templates = configWrapper.getTemplates();

      this.renderedDocument = ConfigurationParser.urlToText(url, document.getInputEncoding());
    } catch (XmlConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new XmlConfigurationException("Error parsing XML configuration at " + url, e);
    }
  }

  /**
   * Constructs an instance of XmlConfiguration from the given XML DOM.
   * <p>
   * The default ClassLoader will first try to use the thread context class loader, followed by the ClassLoader that
   * loaded the Ehcache classes.
   *
   * @param xml XML Document Object Model
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(Document xml) throws XmlConfigurationException {
    this(xml, ClassLoading.getDefaultClassLoader());
  }

  /**
   * Constructs an instance of XmlConfiguration from the given XML DOM and using the provided {@code classLoader} to
   * load user types (e.g. key and value Class instances).
   *
   * @param xml XML Document Object Model
   * @param classLoader ClassLoader to use to load user types.
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(Document xml, ClassLoader classLoader) throws XmlConfigurationException {
    this(xml, classLoader, emptyMap());
  }

  /**
   * Constructs an instance of XmlConfiguration from the given XML DOM and using the provided {@code classLoader} to
   * load user types (e.g. key and value Class instances). The {@code cacheClassLoaders} will let you specify a
   * different {@link java.lang.ClassLoader} to use for each {@link org.ehcache.Cache} managed by the
   * {@link org.ehcache.CacheManager} configured using this {@link org.ehcache.xml.XmlConfiguration}. Caches with
   * aliases that do not appear in the map will use {@code classLoader} as a default.
   *
   * @param xml XML Document Object Model
   * @param classLoader ClassLoader to use to load user types.
   * @param cacheClassLoaders the map with mappings between cache names and the corresponding class loaders
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(Document xml, ClassLoader classLoader, Map<String, ClassLoader> cacheClassLoaders) throws XmlConfigurationException {
    requireNonNull(xml, "The source-element cannot be null");
    requireNonNull(classLoader, "The classLoader can not be null");
    this.cacheClassLoaders = requireNonNull(cacheClassLoaders, "The cacheClassLoaders map can not be null");

    this.source = null;
    try {
      ConfigurationParser parser = new ConfigurationParser();
      this.document = xml;
      ConfigurationParser.XmlConfigurationWrapper configWrapper = parser.documentToConfig(document, classLoader, cacheClassLoaders);
      this.configuration = configWrapper.getConfiguration();
      this.templates = configWrapper.getTemplates();

      this.renderedDocument = documentToText(xml);
    } catch (XmlConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new XmlConfigurationException("Error parsing XML configuration", e);
    }
  }

  /**
   * Constructs an instance of XmlConfiguration from an existing configuration object.
   *
   * @param configuration existing configuration
   *
   * @throws XmlConfigurationException if anything went wrong converting to XML
   */
  public XmlConfiguration(Configuration configuration) throws XmlConfigurationException {
    this.source = null;
    this.cacheClassLoaders = emptyMap();
    try {
      ConfigurationParser parser = new ConfigurationParser();
      this.configuration = configuration;
      this.templates = emptyMap();

      this.document = parser.configToDocument(configuration);
      this.renderedDocument = documentToText(document);
    } catch (XmlConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new XmlConfigurationException("Error unparsing configuration: " + configuration, e);
    }
  }

  /**
   * Return this configuration as an XML {@link org.w3c.dom.Document}.
   *
   * @return configuration XML DOM.
   */
  public Document asDocument() {
    return document;
  }

  /**
   * Return this configuration as a rendered XML string.
   *
   * @return configuration XML string
   */
  public String asRenderedDocument() {
    return renderedDocument;
  }

  @Override
  public String toString() {
    return asRenderedDocument();
  }

  /**
   * Exposes the URL where the XML file parsed or yet to be parsed was or will be sourced from.
   * @return The URL provided at object instantiation
   */
  public URL getURL() {
    return source;
  }

  /**
   * Creates a new {@link CacheConfigurationBuilder} seeded with the cache-template configuration
   * by the given {@code name} in the parsed XML configuration.
   * <p>
   * Note that this version does not specify resources, which are mandatory to create a
   * {@link CacheConfigurationBuilder}. So if the template does not define resources, this will throw.
   *
   * @param name the unique name identifying the cache-template element in the XML
   * @param keyType the type of keys for the {@link CacheConfigurationBuilder} to use, must
   *                match the {@code key-type} declared in the template if declared in XML
   * @param valueType the type of values for the {@link CacheConfigurationBuilder} to use, must
   *                  match the {@code value-type} declared in the template if declared in XML
   * @param <K> type of keys
   * @param <V> type of values
   *
   * @return the preconfigured {@link CacheConfigurationBuilder}
   *         or {@code null} if no cache-template for the provided {@code name}
   *
   * @throws IllegalStateException if the template does not configure resources.
   * @throws IllegalArgumentException if {@code keyType} or {@code valueType} don't match the declared type(s) of the template
   * @throws ClassNotFoundException if a {@link java.lang.Class} declared in the XML couldn't be found
   * @throws InstantiationException if a user provided {@link java.lang.Class} couldn't get instantiated
   * @throws IllegalAccessException if a method (including constructor) couldn't be invoked on a user provided type
   */
  @SuppressWarnings("unchecked")
  public <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilderFromTemplate(final String name,
                                                                                         final Class<K> keyType,
                                                                                         final Class<V> valueType)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    Template template = templates.get(name);
    if (template == null) {
      return null;
    } else {
      return template.builderFor(cacheClassLoaders.getOrDefault(name, getClassLoader()), keyType, valueType, null);
    }
  }

  /**
   * Creates a new {@link CacheConfigurationBuilder} seeded with the cache-template configuration
   * by the given {@code name} in the parsed XML configuration.
   *
   * @param name the unique name identifying the cache-template element in the XML
   * @param keyType the type of keys for the {@link CacheConfigurationBuilder} to use, must
   *                match the {@code key-type} declared in the template if declared in XML
   * @param valueType the type of values for the {@link CacheConfigurationBuilder} to use, must
   *                  match the {@code value-type} declared in the template if declared in XML
   * @param resourcePools Resources definitions that will be used
   * @param <K> type of keys
   * @param <V> type of values
   *
   * @return the preconfigured {@link CacheConfigurationBuilder}
   *         or {@code null} if no cache-template for the provided {@code name}
   *
   * @throws IllegalArgumentException if {@code keyType} or {@code valueType} don't match the declared type(s) of the template
   * @throws ClassNotFoundException if a {@link java.lang.Class} declared in the XML couldn't be found
   * @throws InstantiationException if a user provided {@link java.lang.Class} couldn't get instantiated
   * @throws IllegalAccessException if a method (including constructor) couldn't be invoked on a user provided type
   */
  @SuppressWarnings("unchecked")
  public <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilderFromTemplate(final String name,
                                                                                         final Class<K> keyType,
                                                                                         final Class<V> valueType,
                                                                                         final ResourcePools resourcePools)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    Template template = templates.get(name);
    if (template == null) {
      return null;
    } else {
      return template.builderFor(cacheClassLoaders.getOrDefault(name, getClassLoader()), keyType, valueType, requireNonNull(resourcePools));
    }
  }

  /**
   * Creates a new {@link CacheConfigurationBuilder} seeded with the cache-template configuration
   * by the given {@code name} in the parsed XML configuration.
   *
   * @param name the unique name identifying the cache-template element in the XML
   * @param keyType the type of keys for the {@link CacheConfigurationBuilder} to use, must
   *                match the {@code key-type} declared in the template if declared in XML
   * @param valueType the type of values for the {@link CacheConfigurationBuilder} to use, must
   *                  match the {@code value-type} declared in the template if declared in XML
   * @param resourcePoolsBuilder Resources definitions that will be used
   * @param <K> type of keys
   * @param <V> type of values
   *
   * @return the preconfigured {@link CacheConfigurationBuilder}
   *         or {@code null} if no cache-template for the provided {@code name}
   *
   * @throws IllegalArgumentException if {@code keyType} or {@code valueType} don't match the declared type(s) of the template
   * @throws ClassNotFoundException if a {@link java.lang.Class} declared in the XML couldn't be found
   * @throws InstantiationException if a user provided {@link java.lang.Class} couldn't get instantiated
   * @throws IllegalAccessException if a method (including constructor) couldn't be invoked on a user provided type
   */
  @SuppressWarnings("unchecked")
  public <K, V> CacheConfigurationBuilder<K, V> newCacheConfigurationBuilderFromTemplate(final String name,
                                                                                         final Class<K> keyType,
                                                                                         final Class<V> valueType,
                                                                                         final Builder<? extends ResourcePools> resourcePoolsBuilder)
      throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    return newCacheConfigurationBuilderFromTemplate(name, keyType, valueType, resourcePoolsBuilder.build());
  }

  @Override
  public Map<String, CacheConfiguration<?, ?>> getCacheConfigurations() {
    return configuration.getCacheConfigurations();
  }

  @Override
  public Collection<ServiceCreationConfiguration<?, ?>> getServiceCreationConfigurations() {
    return configuration.getServiceCreationConfigurations();
  }

  @Override
  public ClassLoader getClassLoader() {
    return configuration.getClassLoader();
  }

  @Override
  public FluentConfigurationBuilder<?> derive() {
    return newConfigurationBuilder(this);
  }

  public interface Template {
    <K, V> CacheConfigurationBuilder<K,V> builderFor(ClassLoader classLoader, Class<K> keyType, Class<V> valueType, ResourcePools resourcePools) throws ClassNotFoundException, InstantiationException, IllegalAccessException;
  }

  public static Class<?> getClassForName(String name, ClassLoader classLoader) throws ClassNotFoundException {
    return PRETTY_FORMATS.stream().filter(p -> p.applies().test(name)).findFirst().map(PrettyClassFormat::lookup).orElseThrow(AssertionError::new).lookup(name, classLoader);
  }

  private static final List<PrettyClassFormat> PRETTY_FORMATS = asList(
    //Primitive Types
    when("boolean"::equals).then((n, l) -> Boolean.TYPE),
    when("byte"::equals).then((n, l) -> Byte.TYPE),
    when("short"::equals).then((n, l) -> Short.TYPE),
    when("int"::equals).then((n, l) -> Integer.TYPE),
    when("long"::equals).then((n, l) -> Long.TYPE),
    when("char"::equals).then((n, l) -> Character.TYPE),
    when("float"::equals).then((n, l) -> Float.TYPE),
    when("double"::equals).then((n, l) -> Double.TYPE),

    //Java Language Array Syntax
    when(n -> n.endsWith("[]")).then((n, l) -> {
      String component = n.split("(\\[\\])+$", 2)[0];
      int dimensions = (n.length() - component.length()) >> 1;
      return Array.newInstance(getClassForName(component, l), new int[dimensions]).getClass();
    }),

    //Inner Classes
    when(n -> n.contains(".")).then((n, l) -> {
      try {
        return forName(n, false, l);
      } catch (ClassNotFoundException e) {
        int innerSeperator = n.lastIndexOf(".");
        if (innerSeperator == -1) {
          throw e;
        } else {
          return forName(n.substring(0, innerSeperator) + "$" + n.substring(innerSeperator + 1), false, l);
        }
      }
    }),

    //Everything Else
    when(n -> true).then((n, l) -> forName(n, false, l))
  );

  interface PrettyClassFormat {

    static Builder when(Predicate<String> predicate) {
      return lookup -> new PrettyClassFormat() {
        @Override
        public Predicate<String> applies() {
          return predicate;
        }

        @Override
        public Lookup lookup() {
          return lookup;
        }
      };
    }

    Predicate<String> applies();

    Lookup lookup();

    interface Builder {
      PrettyClassFormat then(Lookup lookup);
    }
  }

  private interface Lookup {

    Class<?> lookup(String name, ClassLoader loader) throws ClassNotFoundException;
  }

}
