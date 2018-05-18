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
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * Exposes {@link org.ehcache.config.Configuration} and {@link CacheConfigurationBuilder} expressed
 * in a XML file that obeys the <a href="http://www.ehcache.org/v3" target="_blank">core Ehcache schema</a>.
 * <p>
 * Instances of this class are not thread-safe.
 */
public class XmlConfiguration implements Configuration {

  private static final Logger LOGGER = LoggerFactory.getLogger(XmlConfiguration.class);

  private final URL xml;
  private final ConfigurationParser parser;
  private final Configuration configuration;
  private final Map<String, CacheTemplate> templates;
  private final String xmlString;

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url}
   * <p>
   * Parses the XML file at the {@code url} provided.
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
   * <p>
   * Parses the XML file at the {@code url} provided.
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
   * the {@link org.ehcache.CacheManager} configured using this {@link org.ehcache.xml.XmlConfiguration}
   * <p>
   * Parses the XML file at the {@code url} provided.
   *
   * @param url URL pointing to the XML file's location
   * @param classLoader ClassLoader to use to load user types.
   * @param cacheClassLoaders the map with mappings between cache names and the corresponding class loaders
   *
   * @throws XmlConfigurationException if anything went wrong parsing the XML
   */
  public XmlConfiguration(URL url, final ClassLoader classLoader, final Map<String, ClassLoader> cacheClassLoaders)
      throws XmlConfigurationException {
    if(url == null) {
      throw new NullPointerException("The url can not be null");
    }
    if(classLoader == null) {
      throw new NullPointerException("The classLoader can not be null");
    }
    if(cacheClassLoaders == null) {
      throw new NullPointerException("The cacheClassLoaders map can not be null");
    }
    this.xml = url;

    try {
      this.parser = new ConfigurationParser();
      ConfigurationParser.XmlConfigurationWrapper xmlConfigurationWrapper = parser.parseConfiguration(url.toExternalForm(), classLoader, cacheClassLoaders);
      configuration = xmlConfigurationWrapper.getConfiguration();
      templates = xmlConfigurationWrapper.getTemplates();
      try (BufferedReader buffer = new BufferedReader(new InputStreamReader(xml.openStream(), Charset.defaultCharset()))) {
        xmlString = buffer.lines().collect(joining("\n"));
      }
    } catch (XmlConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new XmlConfigurationException("Error parsing XML configuration at " + url, e);
    }
  }

  public XmlConfiguration(Configuration configuration) {
    this.xml = null;
    this.configuration = configuration;
    this.templates = emptyMap();

    try {
      this.parser = new ConfigurationParser();
      this.xmlString = parser.unparseConfiguration(configuration);
    } catch (XmlConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new XmlConfigurationException("Error unparsing configuration: " + configuration, e);
    }
  }

  @Override
  public String toString() {
    return xmlString;
  }

  /**
   * Exposes the URL where the XML file parsed or yet to be parsed was or will be sourced from.
   * @return The URL provided at object instantiation
   */
  public URL getURL() {
    return xml;
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
    return internalCacheConfigurationBuilderFromTemplate(name, keyType, valueType, null);
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
    return internalCacheConfigurationBuilderFromTemplate(name, keyType, valueType, resourcePools);
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
    return internalCacheConfigurationBuilderFromTemplate(name, keyType, valueType, resourcePoolsBuilder.build());
  }

  @SuppressWarnings("unchecked")
  private <K, V> CacheConfigurationBuilder<K, V> internalCacheConfigurationBuilderFromTemplate(String name,
                                                                                               Class<K> keyType,
                                                                                               Class<V> valueType,
                                                                                               ResourcePools resourcePools)
        throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    final CacheTemplate cacheTemplate = templates.get(name);
    if (cacheTemplate == null) {
      return null;
    }

    checkTemplateTypeConsistency("key", keyType, cacheTemplate);
    checkTemplateTypeConsistency("value", valueType, cacheTemplate);

    if ((resourcePools == null || resourcePools.getResourceTypeSet().isEmpty()) && cacheTemplate.getHeap() == null && cacheTemplate.getResources().isEmpty()) {
      throw new IllegalStateException("Template defines no resources, and none were provided");
    }

    if (resourcePools == null) {
      resourcePools = parser.getResourceConfigurationParser().parseResourceConfiguration(cacheTemplate, newResourcePoolsBuilder());
    }

    return parser.parseServiceConfigurations(newCacheConfigurationBuilder(keyType, valueType, resourcePools), ClassLoading.getDefaultClassLoader(), cacheTemplate);
  }

  private static <T> void checkTemplateTypeConsistency(String type, Class<T> providedType, CacheTemplate template) throws ClassNotFoundException {
    ClassLoader defaultClassLoader = ClassLoading.getDefaultClassLoader();
    Class<?> templateType;
    if (type.equals("key")) {
      templateType = getClassForName(template.keyType(), defaultClassLoader);
    } else {
      templateType = getClassForName(template.valueType(), defaultClassLoader);
    }

    if(providedType == null || !templateType.isAssignableFrom(providedType)) {
      throw new IllegalArgumentException("CacheTemplate '" + template.id() + "' declares " + type + " type of " + templateType.getName() + ". Provided: " + providedType);
    }
  }

  public static Class<?> getClassForName(String name, ClassLoader classLoader) throws ClassNotFoundException {
    return Class.forName(name, true, classLoader);
  }

  @Override
  public Map<String, CacheConfiguration<?, ?>> getCacheConfigurations() {
    return configuration.getCacheConfigurations();
  }

  @Override
  public Collection<ServiceCreationConfiguration<?>> getServiceCreationConfigurations() {
    return configuration.getServiceCreationConfigurations();
  }

  @Override
  public ClassLoader getClassLoader() {
    return configuration.getClassLoader();
  }
}
