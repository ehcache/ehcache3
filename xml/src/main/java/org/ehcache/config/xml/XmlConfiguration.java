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

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.Configuration;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.EvictionVeto;
import org.ehcache.config.copy.DefaultCopierConfiguration;
import org.ehcache.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.config.event.CacheEventListenerConfigurationBuilder;
import org.ehcache.config.ResourcePool;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.config.persistence.CacheManagerPersistenceConfiguration;
import org.ehcache.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.config.writebehind.WriteBehindConfigurationBuilder;
import org.ehcache.config.xml.ConfigurationParser.WriteBehind;
import org.ehcache.config.xml.model.CopierType;
import org.ehcache.config.xml.model.SerializerType;
import org.ehcache.config.xml.model.ServiceType;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.config.xml.model.EventType;
import org.ehcache.event.CacheEventListener;
import org.ehcache.event.EventFiring;
import org.ehcache.event.EventOrdering;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.util.ClassLoading;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Element;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.ehcache.config.ResourcePoolsBuilder.newResourcePoolsBuilder;

/**
 * Exposes {@link org.ehcache.config.Configuration} and {@link org.ehcache.config.CacheConfigurationBuilder} expressed
 * in a XML file that obeys the ehcache-core.xsd (todo link this to proper location, wherever this ends up being)
 * <p>
 * Instances of this class are not thread-safe
 *
 * @author Chris Dennis
 * @author Alex Snaps
 */
public class XmlConfiguration implements Configuration {

  private static final URL CORE_SCHEMA_URL = XmlConfiguration.class.getResource("/ehcache-core.xsd");
  private static final Logger LOGGER = LoggerFactory.getLogger(XmlConfiguration.class);

  private final URL xml;
  private final ClassLoader classLoader;
  private final Map<String, ClassLoader> cacheClassLoaders;

  private final Collection<ServiceCreationConfiguration<?>> serviceConfigurations = new ArrayList<ServiceCreationConfiguration<?>>();
  private final Map<String, CacheConfiguration<?, ?>> cacheConfigurations = new HashMap<String, CacheConfiguration<?, ?>>();
  private final Map<String, ConfigurationParser.CacheTemplate> templates = new HashMap<String, ConfigurationParser.CacheTemplate>();

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url}
   * <p>
   * Parses the XML file at the {@code url} provided.
   *
   * @param url URL pointing to the XML file's location
   *
   * @throws IOException if anything went wrong accessing the URL
   * @throws SAXException if anything went wrong parsing or validating the XML
   * @throws ClassNotFoundException if a {@link java.lang.Class} declared in the XML couldn't be found
   * @throws InstantiationException if a user provided {@link java.lang.Class} couldn't get instantiated
   * @throws IllegalAccessException if a method (including constructor) couldn't be invoked on a user provided type
   */
  public XmlConfiguration(URL url)
      throws ClassNotFoundException, SAXException, InstantiationException, IllegalAccessException, IOException {
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
   * @throws IOException if anything went wrong accessing the URL
   * @throws SAXException if anything went wrong parsing or validating the XML
   * @throws ClassNotFoundException if a {@link java.lang.Class} declared in the XML couldn't be found
   * @throws InstantiationException if a user provided {@link java.lang.Class} couldn't get instantiated
   * @throws IllegalAccessException if a method (including constructor) couldn't be invoked on a user provided type
   */
  public XmlConfiguration(URL url, final ClassLoader classLoader)
      throws ClassNotFoundException, SAXException, InstantiationException, IOException, IllegalAccessException {
    this(url, classLoader, Collections.<String, ClassLoader>emptyMap());
  }

  /**
   * Constructs an instance of XmlConfiguration mapping to the XML file located at {@code url} and using the provided
   * {@code classLoader} to load user types (e.g. key and value Class instances). The {@code cacheClassLoaders} will
   * let you specify a different {@link java.lang.ClassLoader} to use for each {@link org.ehcache.Cache} managed by
   * the {@link org.ehcache.CacheManager} configured using this {@link org.ehcache.config.xml.XmlConfiguration}
   * <p>
   * Parses the XML file at the {@code url} provided.
   *
   * @param url URL pointing to the XML file's location
   * @param classLoader ClassLoader to use to load user types.
   *
   * @throws IOException if anything went wrong accessing the URL
   * @throws SAXException if anything went wrong parsing or validating the XML
   * @throws ClassNotFoundException if a {@link java.lang.Class} declared in the XML couldn't be found
   * @throws InstantiationException if a user provided {@link java.lang.Class} couldn't get instantiated
   * @throws IllegalAccessException if a method (including constructor) couldn't be invoked on a user provided type
   */
  public XmlConfiguration(URL url, final ClassLoader classLoader, final Map<String, ClassLoader> cacheClassLoaders)
      throws ClassNotFoundException, SAXException, InstantiationException, IllegalAccessException, IOException {
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
    this.cacheClassLoaders = new HashMap<String, ClassLoader>(cacheClassLoaders);
    parseConfiguration();
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
  private void parseConfiguration()
      throws ClassNotFoundException, IOException, SAXException, InstantiationException, IllegalAccessException {
    LOGGER.info("Loading Ehcache XML configuration from {}.", xml.getPath());
    ConfigurationParser configurationParser = new ConfigurationParser(xml.toExternalForm(), CORE_SCHEMA_URL);

    final ArrayList<ServiceCreationConfiguration<?>> serviceConfigs = new ArrayList<ServiceCreationConfiguration<?>>();

    for (ServiceType serviceType : configurationParser.getServiceElements()) {
      if (serviceType.getDefaultSerializers() != null) {
        DefaultSerializationProviderConfiguration configuration = new DefaultSerializationProviderConfiguration();

        for (SerializerType.Serializer serializer : serviceType.getDefaultSerializers().getSerializer()) {
          try {
            configuration.addSerializerFor(getClassForName(serializer.getType(), classLoader), (Class) getClassForName(serializer.getValue(), classLoader));
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
        serviceConfigs.add(configuration);
      } else if(serviceType.getDefaultCopiers() != null) {
        DefaultCopyProviderConfiguration configuration = new DefaultCopyProviderConfiguration();

        for (CopierType.Copier copier : serviceType.getDefaultCopiers().getCopier()) {
          try {
            configuration.addCopierFor(getClassForName(copier.getType(), classLoader), (Class)getClassForName(copier.getValue(), classLoader));
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
        serviceConfigs.add(configuration);
      } else if (serviceType.getPersistence() != null) {
        serviceConfigs.add(new CacheManagerPersistenceConfiguration(new File(serviceType.getPersistence().getDirectory())));
      } else {
        final ServiceCreationConfiguration<?> serviceConfiguration1 = configurationParser.parseExtension((Element)serviceType.getAny());
        serviceConfigs.add(serviceConfiguration1);
      }
    }

    for (ServiceCreationConfiguration<?> serviceConfiguration : Collections.unmodifiableList(serviceConfigs)) {
      serviceConfigurations.add(serviceConfiguration);
    }

    for (ConfigurationParser.CacheDefinition cacheDefinition : configurationParser.getCacheElements()) {
      CacheConfigurationBuilder<Object, Object> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
      String alias = cacheDefinition.id();

      ClassLoader cacheClassLoader = cacheClassLoaders.get(alias);
      if (cacheClassLoader != null) {
        builder = builder.withClassLoader(cacheClassLoader);
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
      EvictionVeto evictionVeto = getInstanceOfName(cacheDefinition.evictionVeto(), cacheClassLoader, EvictionVeto.class);
      EvictionPrioritizer evictionPrioritizer = getInstanceOfName(cacheDefinition.evictionPrioritizer(), cacheClassLoader, EvictionPrioritizer.class, Eviction.Prioritizer.class);
      final ConfigurationParser.Expiry parsedExpiry = cacheDefinition.expiry();
      if (parsedExpiry != null) {
        builder = builder.withExpiry(getExpiry(cacheClassLoader, parsedExpiry));
      }
      ResourcePoolsBuilder resourcePoolsBuilder = newResourcePoolsBuilder();
      for (ResourcePool resourcePool : cacheDefinition.resourcePools()) {
        resourcePoolsBuilder = resourcePoolsBuilder.with(resourcePool.getType(), resourcePool.getSize(), resourcePool.getUnit(), resourcePool.isPersistent());
      }
      builder = builder.withResourcePools(resourcePoolsBuilder);
      for (ServiceConfiguration<?> serviceConfig : cacheDefinition.serviceConfigs()) {
        builder = builder.add(serviceConfig);
      }
      if(cacheDefinition.loaderWriter()!= null) {
        final Class<CacheLoaderWriter<?, ?>> cacheLoaderWriterClass = (Class<CacheLoaderWriter<?,?>>)getClassForName(cacheDefinition.loaderWriter(), cacheClassLoader);
        builder = builder.add(new DefaultCacheLoaderWriterConfiguration(cacheLoaderWriterClass));
        if(cacheDefinition.writeBehind() != null) {
          WriteBehind writeBehind = cacheDefinition.writeBehind();
          WriteBehindConfigurationBuilder writeBehindConfigurationBuilder = WriteBehindConfigurationBuilder.newWriteBehindConfiguration()
              .concurrencyLevel(writeBehind.concurrency()).queueSize(writeBehind.maxQueueSize()).rateLimit(writeBehind.rateLimitPerSecond())
              .delay(writeBehind.minWriteDelay(), writeBehind.maxWriteDelay())
              .batchSize(writeBehind.batchSize());
          if (writeBehind.isRetryAttemptsSet()) {
            writeBehindConfigurationBuilder = writeBehindConfigurationBuilder.retry(writeBehind.retryAttempts(), writeBehind.retryAttemptsDelay());
          }
          if(writeBehind.isCoalesced()) {
            writeBehindConfigurationBuilder = writeBehindConfigurationBuilder.enableCoalescing();
          }
          builder = builder.add(writeBehindConfigurationBuilder);
        }
      }
      if(cacheDefinition.listeners()!= null) {
        for (ConfigurationParser.Listener listener : cacheDefinition.listeners()) {
          final Class<CacheEventListener<?, ?>> cacheEventListenerClass = (Class<CacheEventListener<?, ?>>) getClassForName(listener.className(), cacheClassLoader);
          final List<EventType> eventListToFireOn = listener.fireOn();
          Set<org.ehcache.event.EventType> eventSetToFireOn = new HashSet<org.ehcache.event.EventType>();
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
      final CacheConfiguration<?, ?> config = builder.buildConfig(keyType, valueType, evictionVeto, evictionPrioritizer);
      cacheConfigurations.put(alias, config);
    }

    templates.putAll(configurationParser.getTemplates());
  }

  private Expiry<? super Object, ? super Object> getExpiry(ClassLoader cacheClassLoader, ConfigurationParser.Expiry parsedExpiry)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    final Expiry<? super Object, ? super Object> expiry;
    if (parsedExpiry.isUserDef()) {
      expiry = getInstanceOfName(parsedExpiry.type(), cacheClassLoader, Expiry.class);
    } else if (parsedExpiry.isTTL()) {
      expiry = Expirations.timeToLiveExpiration(new Duration(parsedExpiry.value(), parsedExpiry.unit()));
    } else if (parsedExpiry.isTTI()) {
      expiry = Expirations.timeToIdleExpiration(new Duration(parsedExpiry.value(), parsedExpiry.unit()));
    } else {
      expiry = Expirations.noExpiration();
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

  @SuppressWarnings("unchecked")
  private static <T> T getInstanceOfName(String name, ClassLoader classLoader, Class<T> type, @SuppressWarnings("rawtypes") Class<? extends Enum> shortcutValues) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    if (name == null) {
      return null;
    }
    try {
      return (T) Enum.valueOf(shortcutValues, name);
    } catch (IllegalArgumentException iae) {
      return getInstanceOfName(name, classLoader, type);
    }
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
   * Creates a new {@link org.ehcache.config.CacheConfigurationBuilder} seeded with the cache-template configuration
   * by the given {@code name} in the XML configuration parsed using {@link #parseConfiguration()}
   *
   * @param name the unique name identifying the cache-template element in the XML
   *
   * @return the preconfigured {@link org.ehcache.config.CacheConfigurationBuilder}
   *         or {@code null} if no cache-template for the provided {@code name}
   *
   * @throws ClassNotFoundException if a {@link java.lang.Class} declared in the XML couldn't be found
   * @throws InstantiationException if a user provided {@link java.lang.Class} couldn't get instantiated
   * @throws IllegalAccessException if a method (including constructor) couldn't be invoked on a user provided type
   */
  public CacheConfigurationBuilder<Object, Object> newCacheConfigurationBuilderFromTemplate(final String name) throws InstantiationException, IllegalAccessException, ClassNotFoundException {
    return newCacheConfigurationBuilderFromTemplate(name, null, null);
  }

  /**
   * Creates a new {@link org.ehcache.config.CacheConfigurationBuilder} seeded with the cache-template configuration
   * by the given {@code name} in the XML configuration parsed using {@link #parseConfiguration()}
   *
   * @param name the unique name identifying the cache-template element in the XML
   * @param keyType the type of keys for the {@link org.ehcache.config.CacheConfigurationBuilder} to use, would need to
   *                match the {@code key-type} declared in the template if declared in XML
   * @param valueType the type of values for the {@link org.ehcache.config.CacheConfigurationBuilder} to use, would need to
   *                  match the {@code value-type} declared in the template if declared in XML
   * @param <K> type of keys
   * @param <V> type of values
   *
   * @return the preconfigured {@link org.ehcache.config.CacheConfigurationBuilder}
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
                                                                                         final Class<V> valueType)
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

    CacheConfigurationBuilder<K, V> builder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    builder = builder
        .usingEvictionPrioritizer(getInstanceOfName(cacheTemplate.evictionPrioritizer(), defaultClassLoader, EvictionPrioritizer.class, Eviction.Prioritizer.class))
        .evictionVeto(getInstanceOfName(cacheTemplate.evictionVeto(), defaultClassLoader, EvictionVeto.class));
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
    final String loaderWriter = cacheTemplate.loaderWriter();
    if(loaderWriter!= null) {
      final Class<CacheLoaderWriter<?, ?>> cacheLoaderWriterClass = (Class<CacheLoaderWriter<?,?>>)getClassForName(loaderWriter, defaultClassLoader);
      builder = builder.add(new DefaultCacheLoaderWriterConfiguration(cacheLoaderWriterClass));
      if(cacheTemplate.writeBehind() != null) {
        WriteBehind writeBehind = cacheTemplate.writeBehind();
        WriteBehindConfigurationBuilder writeBehindConfigurationBuilder = WriteBehindConfigurationBuilder.newWriteBehindConfiguration()
            .concurrencyLevel(writeBehind.concurrency()).queueSize(writeBehind.maxQueueSize()).rateLimit(writeBehind.rateLimitPerSecond())
            .delay(writeBehind.minWriteDelay(), writeBehind.maxWriteDelay())
            .batchSize(writeBehind.batchSize());
        if (writeBehind.isRetryAttemptsSet()) {
          writeBehindConfigurationBuilder = writeBehindConfigurationBuilder.retry(writeBehind.retryAttempts(), writeBehind.retryAttemptsDelay());
        }
        if(writeBehind.isCoalesced()) {
          writeBehindConfigurationBuilder = writeBehindConfigurationBuilder.enableCoalescing();
        }
        builder = builder.add(writeBehindConfigurationBuilder);
      }
    }
    if(cacheTemplate.listeners()!= null) {
      for (ConfigurationParser.Listener listener : cacheTemplate.listeners()) {
        final Class<CacheEventListener<?, ?>> cacheEventListenerClass = (Class<CacheEventListener<?, ?>>)getClassForName(listener.className(), defaultClassLoader);
        final List<EventType> eventListToFireOn = listener.fireOn();
        Set<org.ehcache.event.EventType> eventSetToFireOn = new HashSet<org.ehcache.event.EventType>();
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
    ResourcePoolsBuilder resourcePoolsBuilder = newResourcePoolsBuilder();
    for (ResourcePool resourcePool : cacheTemplate.resourcePools()) {
      resourcePoolsBuilder = resourcePoolsBuilder.with(resourcePool.getType(), resourcePool.getSize(), resourcePool.getUnit(), resourcePool.isPersistent());
    }
    builder = builder.withResourcePools(resourcePoolsBuilder);
    for (ServiceConfiguration<?> serviceConfiguration : cacheTemplate.serviceConfigs()) {
      builder = builder.add(serviceConfiguration);
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
