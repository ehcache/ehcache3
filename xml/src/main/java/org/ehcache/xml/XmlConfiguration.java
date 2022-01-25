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
import org.ehcache.config.builders.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder;
import org.ehcache.config.builders.WriteBehindConfigurationBuilder.BatchedWriteBehindConfigurationBuilder;
import org.ehcache.core.config.ExpiryUtils;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.config.event.CacheEventDispatcherFactoryConfiguration;
import org.ehcache.impl.config.event.DefaultCacheEventDispatcherConfiguration;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.impl.config.loaderwriter.writebehind.WriteBehindProviderConfiguration;
import org.ehcache.impl.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.impl.config.resilience.DefaultResilienceStrategyConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreProviderConfiguration;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.resilience.ResilienceStrategy;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.ConfigurationParser.Batching;
import org.ehcache.xml.ConfigurationParser.WriteBehind;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.ehcache.xml.model.CopierType;
import org.ehcache.xml.model.EventType;
import org.ehcache.xml.model.SerializerType;
import org.ehcache.xml.model.ServiceType;
import org.ehcache.xml.model.ThreadPoolReferenceType;
import org.ehcache.xml.model.ThreadPoolsType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private final ClassLoader classLoader;
  private final Map<String, ClassLoader> cacheClassLoaders;

  private final Collection<ServiceCreationConfiguration<?>> serviceConfigurations = new ArrayList<>();
  private final Map<String, CacheConfiguration<?, ?>> cacheConfigurations = new HashMap<>();
  private final Map<String, ConfigurationParser.CacheTemplate> templates = new HashMap<>();

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
    this.classLoader = classLoader;
    this.cacheClassLoaders = new HashMap<>(cacheClassLoaders);
    try {
      parseConfiguration();
    } catch (XmlConfigurationException e) {
      throw e;
    } catch (Exception e) {
      throw new XmlConfigurationException("Error parsing XML configuration at " + url, e);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void parseConfiguration()
      throws ClassNotFoundException, IOException, SAXException, InstantiationException, IllegalAccessException, JAXBException, ParserConfigurationException {
    LOGGER.info("Loading Ehcache XML configuration from {}.", xml.getPath());
    ConfigurationParser configurationParser = new ConfigurationParser(xml.toExternalForm());

    final ArrayList<ServiceCreationConfiguration<?>> serviceConfigs = new ArrayList<>();

    for (ServiceType serviceType : configurationParser.getServiceElements()) {
      final ServiceCreationConfiguration<?> serviceConfiguration = configurationParser.parseExtension(serviceType.getServiceCreationConfiguration());
        serviceConfigs.add(serviceConfiguration);
    }

    if (configurationParser.getDefaultSerializers() != null) {
      DefaultSerializationProviderConfiguration configuration = new DefaultSerializationProviderConfiguration();

      for (SerializerType.Serializer serializer : configurationParser.getDefaultSerializers().getSerializer()) {
        configuration.addSerializerFor(getClassForName(serializer.getType(), classLoader), (Class) getClassForName(serializer.getValue(), classLoader));
      }
      serviceConfigs.add(configuration);
    }
    if (configurationParser.getDefaultCopiers() != null) {
      DefaultCopyProviderConfiguration configuration = new DefaultCopyProviderConfiguration();

      for (CopierType.Copier copier : configurationParser.getDefaultCopiers().getCopier()) {
        configuration.addCopierFor(getClassForName(copier.getType(), classLoader), (Class)getClassForName(copier.getValue(), classLoader));
      }
      serviceConfigs.add(configuration);
    }
    if (configurationParser.getHeapStore() != null) {
      DefaultSizeOfEngineProviderConfiguration configuration = new DefaultSizeOfEngineProviderConfiguration(
              configurationParser.getHeapStore().getMaxObjectSize(), configurationParser.getHeapStore().getUnit(),
              configurationParser.getHeapStore().getMaxObjectGraphSize());
      serviceConfigs.add(configuration);
    }
    if (configurationParser.getPersistence() != null) {
      serviceConfigs.add(new CacheManagerPersistenceConfiguration(new File(configurationParser.getPersistence().getDirectory())));
    }
    if (configurationParser.getThreadPools() != null) {
      PooledExecutionServiceConfiguration poolsConfiguration = new PooledExecutionServiceConfiguration();
      for (ThreadPoolsType.ThreadPool pool : configurationParser.getThreadPools().getThreadPool()) {
        if (pool.isDefault()) {
          poolsConfiguration.addDefaultPool(pool.getAlias(), pool.getMinSize().intValue(), pool.getMaxSize().intValue());
        } else {
          poolsConfiguration.addPool(pool.getAlias(), pool.getMinSize().intValue(), pool.getMaxSize().intValue());
        }
      }
      serviceConfigs.add(poolsConfiguration);
    }
    if (configurationParser.getEventDispatch() != null) {
      ThreadPoolReferenceType eventDispatchThreading = configurationParser.getEventDispatch();
      serviceConfigs.add(new CacheEventDispatcherFactoryConfiguration(eventDispatchThreading.getThreadPool()));
    }
    if (configurationParser.getWriteBehind() != null) {
      ThreadPoolReferenceType writeBehindThreading = configurationParser.getWriteBehind();
      serviceConfigs.add(new WriteBehindProviderConfiguration(writeBehindThreading.getThreadPool()));
    }
    if (configurationParser.getDiskStore() != null) {
      ThreadPoolReferenceType diskStoreThreading = configurationParser.getDiskStore();
      serviceConfigs.add(new OffHeapDiskStoreProviderConfiguration(diskStoreThreading.getThreadPool()));
    }

    serviceConfigurations.addAll(serviceConfigs);

    for (ConfigurationParser.CacheDefinition cacheDefinition : configurationParser.getCacheElements()) {
      String alias = cacheDefinition.id();
      if(cacheConfigurations.containsKey(alias)) {
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
      CacheConfigurationBuilder<Object, Object> builder = newCacheConfigurationBuilder(keyType, valueType, resourcePoolsBuilder);
      if (classLoaderConfigured) {
        builder = builder.withClassLoader(cacheClassLoader);
      }

      if (cacheDefinition.keySerializer() != null) {
        Class keySerializer = getClassForName(cacheDefinition.keySerializer(), cacheClassLoader);
        builder = builder.add(new DefaultSerializerConfiguration(keySerializer, DefaultSerializerConfiguration.Type.KEY));
      }
      if (cacheDefinition.keyCopier() != null) {
        Class keyCopier = getClassForName(cacheDefinition.keyCopier(), cacheClassLoader);
        builder = builder.add(new DefaultCopierConfiguration(keyCopier, DefaultCopierConfiguration.Type.KEY));
      }
      if (cacheDefinition.valueSerializer() != null) {
        Class valueSerializer = getClassForName(cacheDefinition.valueSerializer(), cacheClassLoader);
        builder = builder.add(new DefaultSerializerConfiguration(valueSerializer, DefaultSerializerConfiguration.Type.VALUE));
      }
      if (cacheDefinition.valueCopier() != null) {
        Class valueCopier = getClassForName(cacheDefinition.valueCopier(), cacheClassLoader);
        builder = builder.add(new DefaultCopierConfiguration(valueCopier, DefaultCopierConfiguration.Type.VALUE));
      }
      if (cacheDefinition.heapStoreSettings() != null) {
        builder = builder.add(new DefaultSizeOfEngineConfiguration(cacheDefinition.heapStoreSettings().getMaxObjectSize(), cacheDefinition.heapStoreSettings().getUnit(),
            cacheDefinition.heapStoreSettings().getMaxObjectGraphSize()));
      }
      EvictionAdvisor evictionAdvisor = getInstanceOfName(cacheDefinition.evictionAdvisor(), cacheClassLoader, EvictionAdvisor.class);
      builder = builder.withEvictionAdvisor(evictionAdvisor);
      final ConfigurationParser.Expiry parsedExpiry = cacheDefinition.expiry();
      if (parsedExpiry != null) {
        builder = builder.withExpiry(getExpiry(cacheClassLoader, parsedExpiry));
      }
      final ConfigurationParser.DiskStoreSettings parsedDiskStoreSettings = cacheDefinition.diskStoreSettings();
      if (parsedDiskStoreSettings != null) {
        builder = builder.add(new OffHeapDiskStoreConfiguration(parsedDiskStoreSettings.threadPool(), parsedDiskStoreSettings.writerConcurrency(), parsedDiskStoreSettings.diskSegments()));
      }
      for (ServiceConfiguration<?> serviceConfig : cacheDefinition.serviceConfigs()) {
        builder = builder.add(serviceConfig);
      }
      if(cacheDefinition.loaderWriter()!= null) {
        final Class<CacheLoaderWriter<?, ?>> cacheLoaderWriterClass = (Class<CacheLoaderWriter<?,?>>)getClassForName(cacheDefinition.loaderWriter(), cacheClassLoader);
        builder = builder.add(new DefaultCacheLoaderWriterConfiguration(cacheLoaderWriterClass));
        if(cacheDefinition.writeBehind() != null) {
          WriteBehind writeBehind = cacheDefinition.writeBehind();
          WriteBehindConfigurationBuilder writeBehindConfigurationBuilder;
          if (writeBehind.batching() == null) {
            writeBehindConfigurationBuilder = WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration();
          } else {
            Batching batching = writeBehind.batching();
            writeBehindConfigurationBuilder = WriteBehindConfigurationBuilder
                    .newBatchedWriteBehindConfiguration(batching.maxDelay(), batching.maxDelayUnit(), batching.batchSize());
            if (batching.isCoalesced()) {
              writeBehindConfigurationBuilder = ((BatchedWriteBehindConfigurationBuilder) writeBehindConfigurationBuilder).enableCoalescing();
            }
          }
          builder = builder.add(writeBehindConfigurationBuilder
                  .useThreadPool(writeBehind.threadPool())
                  .concurrencyLevel(writeBehind.concurrency())
                  .queueSize(writeBehind.maxQueueSize()));
        }
      }
      if (cacheDefinition.resilienceStrategy() != null) {
        Class<ResilienceStrategy<?, ?>> resilienceStrategyClass = (Class<ResilienceStrategy<?, ?>>) getClassForName(cacheDefinition.resilienceStrategy(), cacheClassLoader);
        builder = builder.add(new DefaultResilienceStrategyConfiguration(resilienceStrategyClass));
      }
      builder = handleListenersConfig(cacheDefinition.listenersConfig(), cacheClassLoader, builder);
      CacheConfiguration<?, ?> config = builder.build();
      cacheConfigurations.put(alias, config);
    }

    templates.putAll(configurationParser.getTemplates());
  }

  @SuppressWarnings({"unchecked", "deprecation"})
  private ExpiryPolicy<? super Object, ? super Object> getExpiry(ClassLoader cacheClassLoader, ConfigurationParser.Expiry parsedExpiry)
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

  private static Class<?> getClassForName(String name, ClassLoader classLoader) throws ClassNotFoundException {
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
   * by the given {@code name} in the XML configuration parsed using {@link #parseConfiguration()}.
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
   * @throws IllegalStateException if {@link #parseConfiguration()} hasn't yet been successfully invoked or the template
   * does not configure resources.
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
   * by the given {@code name} in the XML configuration parsed using {@link #parseConfiguration()}.
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
   * @throws IllegalStateException if {@link #parseConfiguration()} hasn't yet been successfully invoked
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
   * by the given {@code name} in the XML configuration parsed using {@link #parseConfiguration()}.
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
   * @throws IllegalStateException if {@link #parseConfiguration()} hasn't yet been successfully invoked
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

    final ConfigurationParser.CacheTemplate cacheTemplate = templates.get(name);
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
    builder = builder
        .withEvictionAdvisor(getInstanceOfName(cacheTemplate.evictionAdvisor(), defaultClassLoader, EvictionAdvisor.class));
    final ConfigurationParser.Expiry parsedExpiry = cacheTemplate.expiry();
    if (parsedExpiry != null) {
      builder = builder.withExpiry(getExpiry(defaultClassLoader, parsedExpiry));
    }

    if (cacheTemplate.keySerializer() != null) {
      final Class<Serializer<?>> keySerializer = (Class<Serializer<?>>) getClassForName(cacheTemplate.keySerializer(), defaultClassLoader);
      builder = builder.add(new DefaultSerializerConfiguration(keySerializer, DefaultSerializerConfiguration.Type.KEY));
    }
    if (cacheTemplate.keyCopier() != null) {
      final Class<Copier<?>> keyCopier = (Class<Copier<?>>) getClassForName(cacheTemplate.keyCopier(), defaultClassLoader);
      builder = builder.add(new DefaultCopierConfiguration(keyCopier, DefaultCopierConfiguration.Type.KEY));
    }
    if (cacheTemplate.valueSerializer() != null) {
      final Class<Serializer<?>> valueSerializer = (Class<Serializer<?>>) getClassForName(cacheTemplate.valueSerializer(), defaultClassLoader);
      builder = builder.add(new DefaultSerializerConfiguration(valueSerializer, DefaultSerializerConfiguration.Type.VALUE));
    }
    if (cacheTemplate.valueCopier() != null) {
      final Class<Copier<?>> valueCopier = (Class<Copier<?>>) getClassForName(cacheTemplate.valueCopier(), defaultClassLoader);
      builder = builder.add(new DefaultCopierConfiguration(valueCopier, DefaultCopierConfiguration.Type.VALUE));
    }
    if (cacheTemplate.heapStoreSettings() != null) {
      builder = builder.add(new DefaultSizeOfEngineConfiguration(cacheTemplate.heapStoreSettings().getMaxObjectSize(), cacheTemplate.heapStoreSettings().getUnit(),
        cacheTemplate.heapStoreSettings().getMaxObjectGraphSize()));
    }
    final String loaderWriter = cacheTemplate.loaderWriter();
    if(loaderWriter!= null) {
      final Class<CacheLoaderWriter<?, ?>> cacheLoaderWriterClass = (Class<CacheLoaderWriter<?,?>>)getClassForName(loaderWriter, defaultClassLoader);
      builder = builder.add(new DefaultCacheLoaderWriterConfiguration(cacheLoaderWriterClass));
      if(cacheTemplate.writeBehind() != null) {
        WriteBehind writeBehind = cacheTemplate.writeBehind();
        WriteBehindConfigurationBuilder writeBehindConfigurationBuilder;
        if (writeBehind.batching() == null) {
          writeBehindConfigurationBuilder = WriteBehindConfigurationBuilder.newUnBatchedWriteBehindConfiguration();
        } else {
          Batching batching = writeBehind.batching();
          writeBehindConfigurationBuilder = WriteBehindConfigurationBuilder.newBatchedWriteBehindConfiguration(batching.maxDelay(), batching.maxDelayUnit(), batching.batchSize());
          if (batching.isCoalesced()) {
            writeBehindConfigurationBuilder = ((BatchedWriteBehindConfigurationBuilder) writeBehindConfigurationBuilder).enableCoalescing();
          }
        }
        builder = builder.add(writeBehindConfigurationBuilder
                .concurrencyLevel(writeBehind.concurrency())
                .queueSize(writeBehind.maxQueueSize()));
      }
    }
    String resilienceStrategy = cacheTemplate.resilienceStrategy();
    if (resilienceStrategy != null) {
      final Class<ResilienceStrategy<?, ?>> resilienceStrategyClass = (Class<ResilienceStrategy<?, ?>>) getClassForName(resilienceStrategy, defaultClassLoader);
      builder = builder.add(new DefaultResilienceStrategyConfiguration(resilienceStrategyClass));
    }
    builder = handleListenersConfig(cacheTemplate.listenersConfig(), defaultClassLoader, builder);
    for (ServiceConfiguration<?> serviceConfiguration : cacheTemplate.serviceConfigs()) {
      builder = builder.add(serviceConfiguration);
    }
    return builder;
  }

  private <K, V> CacheConfigurationBuilder<K, V> handleListenersConfig(ConfigurationParser.ListenersConfig listenersConfig, ClassLoader defaultClassLoader, CacheConfigurationBuilder<K, V> builder) throws ClassNotFoundException {
    if(listenersConfig != null) {
      if (listenersConfig.threadPool() != null) {
        builder = builder.add(new DefaultCacheEventDispatcherConfiguration(listenersConfig.threadPool()));
      }
      if (listenersConfig.listeners() != null) {
        for (ConfigurationParser.Listener listener : listenersConfig.listeners()) {
          @SuppressWarnings("unchecked")
          final Class<CacheEventListener<?, ?>> cacheEventListenerClass = (Class<CacheEventListener<?, ?>>)getClassForName(listener.className(), defaultClassLoader);
          final List<EventType> eventListToFireOn = listener.fireOn();
          Set<org.ehcache.event.EventType> eventSetToFireOn = new HashSet<>();
          for (EventType events : eventListToFireOn) {
            switch (events) {
              case CREATED:
                eventSetToFireOn.add(org.ehcache.event.EventType.CREATED);
                break;
              case EVICTED:
                eventSetToFireOn.add(org.ehcache.event.EventType.EVICTED);
                break;
              case EXPIRED:
                eventSetToFireOn.add(org.ehcache.event.EventType.EXPIRED);
                break;
              case UPDATED:
                eventSetToFireOn.add(org.ehcache.event.EventType.UPDATED);
                break;
              case REMOVED:
                eventSetToFireOn.add(org.ehcache.event.EventType.REMOVED);
                break;
              default:
                throw new IllegalArgumentException("Invalid Event Type provided");
            }
          }
          CacheEventListenerConfigurationBuilder listenerBuilder = CacheEventListenerConfigurationBuilder
              .newEventListenerConfiguration(cacheEventListenerClass, eventSetToFireOn)
              .firingMode(EventFiring.valueOf(listener.eventFiring().value()))
              .eventOrdering(EventOrdering.valueOf(listener.eventOrdering().value()));
          builder = builder.add(listenerBuilder);
        }
      }
    }
    return builder;
  }

  @Override
  public Map<String, CacheConfiguration<?, ?>> getCacheConfigurations() {
    return cacheConfigurations;
  }

  @Override
  public Collection<ServiceCreationConfiguration<?>> getServiceCreationConfigurations() {
    return serviceConfigurations;
  }

  @Override
  public ClassLoader getClassLoader() {
    return classLoader;
  }
}
