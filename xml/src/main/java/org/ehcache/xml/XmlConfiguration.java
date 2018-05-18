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
import org.ehcache.config.EvictionAdvisor;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePools;
import org.ehcache.config.Builder;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.ConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CacheDefinition;
import org.ehcache.xml.model.CacheTemplate;
import org.ehcache.xml.model.ConfigType;
import org.ehcache.xml.model.Expiry;
import org.ehcache.xml.model.ServiceType;
import org.ehcache.xml.provider.DefaultCopyProviderConfigurationParser;
import org.ehcache.xml.provider.DefaultSerializationProviderConfigurationParser;
import org.ehcache.xml.provider.OffHeapDiskStoreProviderConfigurationParser;
import org.ehcache.xml.provider.CacheEventDispatcherFactoryConfigurationParser;
import org.ehcache.xml.provider.DefaultSizeOfEngineProviderConfigurationParser;
import org.ehcache.xml.provider.CacheManagerPersistenceConfigurationParser;
import org.ehcache.xml.provider.PooledExecutionServiceConfigurationParser;
import org.ehcache.xml.provider.WriteBehindProviderConfigurationParser;
import org.ehcache.xml.service.DefaultCacheEventDispatcherConfigurationParser;
import org.ehcache.xml.service.DefaultCacheEventListenerConfigurationParser;
import org.ehcache.xml.service.DefaultCacheLoaderWriterConfigurationParser;
import org.ehcache.xml.service.DefaultResilienceStrategyConfigurationParser;
import org.ehcache.xml.service.DefaultSizeOfEngineConfigurationParser;
import org.ehcache.xml.service.DefaultWriteBehindConfigurationParser;
import org.ehcache.xml.service.OffHeapDiskStoreConfigurationParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ConfigurationBuilder.newConfigurationBuilder;
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
  private final Configuration configuration;
  private final Map<String, CacheTemplate> templates = new HashMap<>();

  private static final Collection<CoreServiceCreationConfigurationParser> CORE_SERVICE_CREATION_CONFIGURATION_PARSERS = asList(
    new DefaultCopyProviderConfigurationParser(),
    new DefaultSerializationProviderConfigurationParser(),
    new OffHeapDiskStoreProviderConfigurationParser(),
    new CacheEventDispatcherFactoryConfigurationParser(),
    new DefaultSizeOfEngineProviderConfigurationParser(),
    new CacheManagerPersistenceConfigurationParser(),
    new PooledExecutionServiceConfigurationParser(),
    new WriteBehindProviderConfigurationParser()
  );

  private static final Collection<CoreServiceConfigurationParser> CORE_SERVICE_CONFIGURATION_PARSERS = asList(
    new DefaultCacheLoaderWriterConfigurationParser(),
    new DefaultResilienceStrategyConfigurationParser(),
    new DefaultSizeOfEngineConfigurationParser(),
    new DefaultWriteBehindConfigurationParser(),
    new OffHeapDiskStoreConfigurationParser(),
    new DefaultCacheEventDispatcherConfigurationParser(),
    new DefaultCacheEventListenerConfigurationParser()
  );

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
      configuration = parseConfiguration(xml, classLoader, cacheClassLoaders);
    } catch (XmlConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new XmlConfigurationException("Error parsing XML configuration at " + url, e);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private Configuration parseConfiguration(URL xml, ClassLoader classLoader, Map<String, ClassLoader> cacheClassLoaders)
      throws ClassNotFoundException, IOException, SAXException, InstantiationException, IllegalAccessException, JAXBException, ParserConfigurationException {
    LOGGER.info("Loading Ehcache XML configuration from {}.", xml.getPath());
    ConfigurationParser configurationParser = new ConfigurationParser(xml.toExternalForm());

    ConfigurationBuilder managerBuilder = newConfigurationBuilder();
    managerBuilder = managerBuilder.withClassLoader(classLoader);

    ConfigType configRoot = configurationParser.getConfigRoot();

    for (CoreServiceCreationConfigurationParser parser : CORE_SERVICE_CREATION_CONFIGURATION_PARSERS) {
      managerBuilder = parser.parseServiceCreationConfiguration(configRoot, classLoader, managerBuilder);
    }

    for (ServiceType serviceType : configRoot.getService()) {
      final ServiceCreationConfiguration<?> serviceConfiguration = configurationParser.parseExtension(serviceType.getServiceCreationConfiguration());
      managerBuilder = managerBuilder.addService(serviceConfiguration);
    }

    templates.putAll(configurationParser.getTemplates());

    for (CacheDefinition cacheDefinition : configurationParser.getCacheElements()) {
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

      Class keyType = getClassForName(cacheDefinition.keyType(), cacheClassLoader);
      Class valueType = getClassForName(cacheDefinition.valueType(), cacheClassLoader);
      ResourcePoolsBuilder resourcePoolsBuilder = newResourcePoolsBuilder();
      for (ResourcePool resourcePool : cacheDefinition.resourcePools()) {
        resourcePoolsBuilder = resourcePoolsBuilder.with(resourcePool);
      }
      CacheConfigurationBuilder<Object, Object> cacheBuilder = newCacheConfigurationBuilder(keyType, valueType, resourcePoolsBuilder);
      if (classLoaderConfigured) {
        cacheBuilder = cacheBuilder.withClassLoader(cacheClassLoader);
      }

      cacheBuilder = buildCacheConfigurationFromTemplate(cacheBuilder, cacheClassLoader, cacheDefinition);
      managerBuilder = managerBuilder.addCache(alias, cacheBuilder.build());
    }

    return managerBuilder.build();
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  private static ExpiryPolicy<? super Object, ? super Object> getExpiry(ClassLoader cacheClassLoader, Expiry parsedExpiry)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final ExpiryPolicy<? super Object, ? super Object> expiry;
    if (parsedExpiry.isUserDef()) {
      ExpiryPolicy<? super Object, ? super Object> tmpExpiry;
      try {
        tmpExpiry = getInstanceOfName(parsedExpiry.type(), cacheClassLoader, ExpiryPolicy.class);
      } catch (ClassCastException e) {
        tmpExpiry = ExpiryUtils.convertToExpiryPolicy(getInstanceOfName(parsedExpiry.type(), cacheClassLoader, org.ehcache.expiry.Expiry.class));
      }
      expiry = tmpExpiry;
    } else if (parsedExpiry.isTTL()) {
      expiry = ExpiryPolicyBuilder.timeToLiveExpiration(Duration.of(parsedExpiry.value(), parsedExpiry.unit()));
    } else if (parsedExpiry.isTTI()) {
      expiry = ExpiryPolicyBuilder.timeToIdleExpiration(Duration.of(parsedExpiry.value(), parsedExpiry.unit()));
    } else {
      expiry = ExpiryPolicyBuilder.noExpiration();
    }
    return expiry;
  }

  private static <T> T getInstanceOfName(String name, ClassLoader classLoader, Class<T> type) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    if (name == null) {
      return null;
    }
    Class<?> klazz = getClassForName(name, classLoader);
    return klazz.asSubclass(type).newInstance();
  }

  public static Class<?> getClassForName(String name, ClassLoader classLoader) throws ClassNotFoundException {
    return Class.forName(name, true, classLoader);
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
    if (resourcePools == null || resourcePools.getResourceTypeSet().isEmpty()) {
      throw new IllegalArgumentException("ResourcePools parameter must define at least one resource");
    }
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
  private <K, V> CacheConfigurationBuilder<K, V> internalCacheConfigurationBuilderFromTemplate(final String name,
                                                                                               final Class<K> keyType,
                                                                                               final Class<V> valueType,
                                                                                               final ResourcePools resourcePools)
        throws InstantiationException, IllegalAccessException, ClassNotFoundException {

    final CacheTemplate cacheTemplate = templates.get(name);
    if (cacheTemplate == null) {
      return null;
    }
    final ClassLoader defaultClassLoader = ClassLoading.getDefaultClassLoader();
    Class keyClass = getClassForName(cacheTemplate.keyType(), defaultClassLoader);
    Class valueClass = getClassForName(cacheTemplate.valueType(), defaultClassLoader);
    if(keyType != null && cacheTemplate.keyType() != null && !keyClass.isAssignableFrom(keyType)) {
      throw new IllegalArgumentException("CacheTemplate '" + name + "' declares key type of " + cacheTemplate.keyType());
    }
    if(valueType != null && cacheTemplate.valueType() != null && !valueClass.isAssignableFrom(valueType)) {
      throw new IllegalArgumentException("CacheTemplate '" + name + "' declares value type of " + cacheTemplate.valueType());
    }

    if ((resourcePools == null || resourcePools.getResourceTypeSet().isEmpty()) && cacheTemplate.resourcePools().isEmpty()) {
      throw new IllegalStateException("Template defines no resources, and none were provided");
    }
    CacheConfigurationBuilder<K, V> builder;
    if (resourcePools != null) {
      builder = newCacheConfigurationBuilder(keyType, valueType, resourcePools);
    } else {
      ResourcePoolsBuilder resourcePoolsBuilder = newResourcePoolsBuilder();
      for (ResourcePool resourcePool : cacheTemplate.resourcePools()) {
        resourcePoolsBuilder = resourcePoolsBuilder.with(resourcePool);
      }
      builder = newCacheConfigurationBuilder(keyType, valueType, resourcePoolsBuilder);
    }

    return buildCacheConfigurationFromTemplate(builder, defaultClassLoader, cacheTemplate);
  }

  private <K, V> CacheConfigurationBuilder<K, V> buildCacheConfigurationFromTemplate(CacheConfigurationBuilder<K, V> cacheBuilder,
                                                                                            ClassLoader cacheClassLoader,
                                                                                            CacheTemplate cacheDefinition) throws ClassNotFoundException, IllegalAccessException, InstantiationException {

    if (cacheDefinition.keySerializer() != null) {
      Class keySerializer = getClassForName(cacheDefinition.keySerializer(), cacheClassLoader);
      cacheBuilder = cacheBuilder.add(new DefaultSerializerConfiguration(keySerializer, DefaultSerializerConfiguration.Type.KEY));
    }
    if (cacheDefinition.keyCopier() != null) {
      Class keyCopier = getClassForName(cacheDefinition.keyCopier(), cacheClassLoader);
      cacheBuilder = cacheBuilder.add(new DefaultCopierConfiguration(keyCopier, DefaultCopierConfiguration.Type.KEY));
    }
    if (cacheDefinition.valueSerializer() != null) {
      Class valueSerializer = getClassForName(cacheDefinition.valueSerializer(), cacheClassLoader);
      cacheBuilder = cacheBuilder.add(new DefaultSerializerConfiguration(valueSerializer, DefaultSerializerConfiguration.Type.VALUE));
    }
    if (cacheDefinition.valueCopier() != null) {
      Class valueCopier = getClassForName(cacheDefinition.valueCopier(), cacheClassLoader);
      cacheBuilder = cacheBuilder.add(new DefaultCopierConfiguration(valueCopier, DefaultCopierConfiguration.Type.VALUE));
    }

    EvictionAdvisor evictionAdvisor = getInstanceOfName(cacheDefinition.evictionAdvisor(), cacheClassLoader, EvictionAdvisor.class);
    cacheBuilder = cacheBuilder.withEvictionAdvisor(evictionAdvisor);
    final Expiry parsedExpiry = cacheDefinition.expiry();
    if (parsedExpiry != null) {
      cacheBuilder = cacheBuilder.withExpiry(getExpiry(cacheClassLoader, parsedExpiry));
    }

    for (CoreServiceConfigurationParser coreServiceConfigParser : CORE_SERVICE_CONFIGURATION_PARSERS) {
      cacheBuilder = coreServiceConfigParser.parseServiceConfiguration(cacheDefinition, cacheClassLoader, cacheBuilder);
    }

    for (ServiceConfiguration<?> serviceConfig : cacheDefinition.serviceConfigs()) {
      cacheBuilder = cacheBuilder.add(serviceConfig);
    }

    return cacheBuilder;
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
