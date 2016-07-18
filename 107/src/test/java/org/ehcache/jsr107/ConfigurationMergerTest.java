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

package org.ehcache.jsr107;

import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.core.internal.service.ServiceLocator;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.config.loaderwriter.DefaultCacheLoaderWriterConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.copy.IdentityCopier;
import org.ehcache.jsr107.config.Jsr107Configuration;
import org.ehcache.jsr107.config.Jsr107Service;
import org.ehcache.jsr107.internal.DefaultJsr107Service;
import org.ehcache.spi.loaderwriter.CacheLoaderWriter;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.internal.creation.MockSettingsImpl;

import java.io.Closeable;
import java.util.Collection;
import java.util.concurrent.TimeUnit;

import javax.cache.configuration.CacheEntryListenerConfiguration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheWriter;

import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.core.internal.util.ValueSuppliers.supplierOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * ConfigurationMergerTest
 */
@SuppressWarnings("unchecked")
public class ConfigurationMergerTest {

  private ConfigurationMerger merger;
  private XmlConfiguration xmlConfiguration;
  private Jsr107Service jsr107Service;
  private Eh107CacheLoaderWriterProvider cacheLoaderWriterFactory;

  @Before
  public void setUp() {
    xmlConfiguration = mock(XmlConfiguration.class);
    jsr107Service = mock(Jsr107Service.class);
    cacheLoaderWriterFactory = mock(Eh107CacheLoaderWriterProvider.class);
    merger = new ConfigurationMerger(xmlConfiguration, jsr107Service, cacheLoaderWriterFactory);
  }

  @Test
  public void mergeConfigNoTemplateNoLoaderWriter() {
    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    ConfigurationMerger.ConfigHolder<Object, Object> configHolder = merger.mergeConfigurations("cache", configuration);

    assertThat(configHolder.cacheResources.getExpiryPolicy().getExpiryForCreation(42L, "Yay!"), is(Duration.INFINITE));
    assertThat(configHolder.cacheResources.getCacheLoaderWriter(), nullValue());
    assertThat(configHolder.useEhcacheLoaderWriter, is(false));

    boolean storeByValue = false;
    Collection<ServiceConfiguration<?>> serviceConfigurations = configHolder.cacheConfiguration.getServiceConfigurations();
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      if (serviceConfiguration instanceof DefaultCopierConfiguration) {
        storeByValue = true;
        break;
      }
    }
    assertThat(storeByValue, is(true));
  }

  @Test
  public void jsr107ExpiryGetsRegistered() {
    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    RecordingFactory<CreatedExpiryPolicy> factory = factoryOf(new CreatedExpiryPolicy(javax.cache.expiry.Duration.FIVE_MINUTES));
    configuration.setExpiryPolicyFactory(factory);

    ConfigurationMerger.ConfigHolder<Object, Object> configHolder = merger.mergeConfigurations("Cache", configuration);

    assertThat(factory.called, is(true));
    Expiry resourcesExpiry = configHolder.cacheResources.getExpiryPolicy();
    Expiry configExpiry = configHolder.cacheConfiguration.getExpiry();
    assertThat(configExpiry, sameInstance(resourcesExpiry));
  }

  @Test
  public void jsr107LoaderGetsRegistered() {
    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    CacheLoader<Object, Object> mock = mock(CacheLoader.class);
    RecordingFactory<CacheLoader<Object, Object>> factory = factoryOf(mock);
    configuration.setReadThrough(true).setCacheLoaderFactory(factory);

    merger.mergeConfigurations("cache", configuration);

    assertThat(factory.called, is(true));
    verify(cacheLoaderWriterFactory).registerJsr107Loader(eq("cache"), Matchers.<CacheLoaderWriter<Object, Object>>anyObject());
  }

  @Test
  public void jsr107WriterGetsRegistered() {
    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    CacheWriter<Object, Object> mock = mock(CacheWriter.class);
    RecordingFactory<CacheWriter<Object, Object>> factory = factoryOf(mock);
    configuration.setWriteThrough(true).setCacheWriterFactory(factory);

    merger.mergeConfigurations("cache", configuration);

    assertThat(factory.called, is(true));
    verify(cacheLoaderWriterFactory).registerJsr107Loader(eq("cache"), Matchers.<CacheLoaderWriter<Object, Object>>anyObject());
  }

  @Test
  public void looksUpTemplateName() {
    merger.mergeConfigurations("cache", new MutableConfiguration<Object, Object>());

    verify(jsr107Service).getTemplateNameForCache("cache");
  }

  @Test
  public void loadsTemplateWhenNameFound() throws Exception {
    when(jsr107Service.getTemplateNameForCache("cache")).thenReturn("cacheTemplate");

    merger.mergeConfigurations("cache", new MutableConfiguration<Object, Object>());

    verify(xmlConfiguration).newCacheConfigurationBuilderFromTemplate("cacheTemplate", Object.class, Object.class);
  }

  @Test
  public void jsr107ExpiryGetsOverriddenByTemplate() throws Exception {
    when(jsr107Service.getTemplateNameForCache("cache")).thenReturn("cacheTemplate");
    when(xmlConfiguration.newCacheConfigurationBuilderFromTemplate("cacheTemplate", Object.class, Object.class)).thenReturn(
        newCacheConfigurationBuilder(Object.class, Object.class, heap(10)).withExpiry(Expirations.timeToLiveExpiration(new Duration(5, TimeUnit.MINUTES)))
    );

    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    RecordingFactory<CreatedExpiryPolicy> factory = factoryOf(new CreatedExpiryPolicy(javax.cache.expiry.Duration.FIVE_MINUTES));
    configuration.setExpiryPolicyFactory(factory);

    ConfigurationMerger.ConfigHolder<Object, Object> configHolder = merger.mergeConfigurations("cache", configuration);

    assertThat(factory.called, is(false));
    Eh107Expiry<Object, Object> expiryPolicy = configHolder.cacheResources.getExpiryPolicy();
    Expiry<? super Object, ? super Object> expiry = configHolder.cacheConfiguration.getExpiry();
    assertThat(expiryPolicy.getExpiryForAccess(42, supplierOf("Yay")), is(expiry.getExpiryForAccess(42, supplierOf("Yay"))));
    assertThat(expiryPolicy.getExpiryForUpdate(42, supplierOf("Yay"), "Lala"), is(expiry.getExpiryForUpdate(42, supplierOf("Yay"), "Lala")));
    assertThat(expiryPolicy.getExpiryForCreation(42, "Yay"), is(expiry.getExpiryForCreation(42, "Yay")));
  }

  @Test
  public void jsr107LoaderGetsOverriddenByTemplate() throws Exception {
    when(jsr107Service.getTemplateNameForCache("cache")).thenReturn("cacheTemplate");
    when(xmlConfiguration.newCacheConfigurationBuilderFromTemplate("cacheTemplate", Object.class, Object.class)).thenReturn(
        newCacheConfigurationBuilder(Object.class, Object.class, heap(10)).add(new DefaultCacheLoaderWriterConfiguration((Class)null))
    );

    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    CacheLoader<Object, Object> mock = mock(CacheLoader.class);
    RecordingFactory<CacheLoader<Object, Object>> factory = factoryOf(mock);
    configuration.setReadThrough(true).setCacheLoaderFactory(factory);

    ConfigurationMerger.ConfigHolder<Object, Object> configHolder = merger.mergeConfigurations("cache", configuration);

    assertThat(factory.called, is(false));
    assertThat(configHolder.cacheResources.getCacheLoaderWriter(), nullValue());
  }

  @Test
  public void jsr107StoreByValueGetsOverriddenByTemplate() throws Exception {
    CacheConfigurationBuilder<Object, Object> builder = newCacheConfigurationBuilder(Object.class, Object.class, heap(10))
        .add(new DefaultCopierConfiguration<Object>((Class)IdentityCopier.class, DefaultCopierConfiguration.Type.KEY))
        .add(new DefaultCopierConfiguration<Object>((Class)IdentityCopier.class, DefaultCopierConfiguration.Type.VALUE));

    when(jsr107Service.getTemplateNameForCache("cache")).thenReturn("cacheTemplate");
    when(xmlConfiguration.newCacheConfigurationBuilderFromTemplate("cacheTemplate", Object.class, Object.class))
        .thenReturn(builder);

    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();  //store-by-value by default

    ConfigurationMerger.ConfigHolder<Object, Object> configHolder = merger.mergeConfigurations("cache", configuration);

    boolean storeByValue = true;
    Collection<ServiceConfiguration<?>> serviceConfigurations = configHolder.cacheConfiguration.getServiceConfigurations();
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      if (serviceConfiguration instanceof DefaultCopierConfiguration) {
        DefaultCopierConfiguration copierConfig = (DefaultCopierConfiguration)serviceConfiguration;
        if(copierConfig.getClazz().isAssignableFrom(IdentityCopier.class))
          storeByValue = false;
        break;
      }
    }
    assertThat(storeByValue, is(false));
  }

  @Test
  public void jsr107LoaderInitFailureClosesExpiry() throws Exception {
    ExpiryPolicy expiryPolicy = mock(ExpiryPolicy.class, new MockSettingsImpl<Object>().extraInterfaces(Closeable.class));

    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    Factory<CacheLoader<Object, Object>> factory = throwingFactory();
    configuration.setExpiryPolicyFactory(factoryOf(expiryPolicy))
        .setReadThrough(true)
        .setCacheLoaderFactory(factory);

    try {
      merger.mergeConfigurations("cache", configuration);
      fail("Loader factory should have thrown");
    } catch (MultiCacheException mce) {
      verify((Closeable) expiryPolicy).close();
    }
  }

  @Test
  public void jsr107ListenerFactoryInitFailureClosesExpiryLoader() throws Exception {
    ExpiryPolicy expiryPolicy = mock(ExpiryPolicy.class, new MockSettingsImpl<Object>().extraInterfaces(Closeable.class));
    CacheLoader<Object, Object> loader = mock(CacheLoader.class, new MockSettingsImpl<Object>().extraInterfaces(Closeable.class));

    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    configuration.setExpiryPolicyFactory(factoryOf(expiryPolicy))
        .setReadThrough(true)
        .setCacheLoaderFactory(factoryOf(loader))
        .addCacheEntryListenerConfiguration(new ThrowingCacheEntryListenerConfiguration());

    try {
      merger.mergeConfigurations("cache", configuration);
      fail("Loader factory should have thrown");
    } catch (MultiCacheException mce) {
      verify((Closeable) expiryPolicy).close();
      verify((Closeable) loader).close();
    }
  }

  @Test
  public void jsr107LoaderInitAlways() {
    CacheLoader<Object, Object> loader = mock(CacheLoader.class);

    MutableConfiguration<Object, Object> configuration = new MutableConfiguration<Object, Object>();
    RecordingFactory<CacheLoader<Object, Object>> factory = factoryOf(loader);
    configuration.setCacheLoaderFactory(factory);

    ConfigurationMerger.ConfigHolder<Object, Object> configHolder = merger.mergeConfigurations("cache", configuration);

    assertThat(factory.called, is(true));
    assertThat(configHolder.cacheResources.getCacheLoaderWriter(), notNullValue());
    assertThat(configHolder.useEhcacheLoaderWriter, is(false));
  }

  @Test
  public void setReadThroughWithoutLoaderFails() {
    MutableConfiguration<Long, String> config = new MutableConfiguration<Long, String>();
    config.setTypes(Long.class, String.class);
    config.setReadThrough(true);

    try {
      merger.mergeConfigurations("cache", config);
      fail("Expected exception as no CacheLoader factory is configured and read-through is enabled.");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("read-through"));
    }
  }

  @Test
  public void setWriteThroughWithoutWriterFails() {
    MutableConfiguration<Long, String> config = new MutableConfiguration<Long, String>();
    config.setTypes(Long.class, String.class);
    config.setWriteThrough(true);

    try {
      merger.mergeConfigurations("cache", config);
      fail("Expected exception as no CacheLoader factory is configured and read-through is enabled.");
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), containsString("write-through"));
    }
  }

  @Test
  public void jsr107DefaultEh107IdentityCopierForImmutableTypes() {
    XmlConfiguration xmlConfiguration = new XmlConfiguration(getClass().getResource("/ehcache-107-copiers-immutable-types.xml"));
    final DefaultJsr107Service jsr107Service = new DefaultJsr107Service(ServiceLocator.findSingletonAmongst(Jsr107Configuration.class, xmlConfiguration.getServiceCreationConfigurations().toArray()));
    merger = new ConfigurationMerger(xmlConfiguration, jsr107Service, mock(Eh107CacheLoaderWriterProvider.class));

    MutableConfiguration<Long, String> stringCacheConfiguration  = new MutableConfiguration<Long, String>();
    stringCacheConfiguration.setTypes(Long.class, String.class);
    ConfigurationMerger.ConfigHolder<Long, String> configHolder1 = merger.mergeConfigurations("stringCache", stringCacheConfiguration);

    assertDefaultCopier(configHolder1.cacheConfiguration.getServiceConfigurations());

    MutableConfiguration<Long, Double> doubleCacheConfiguration  = new MutableConfiguration<Long, Double>();
    doubleCacheConfiguration.setTypes(Long.class, Double.class);
    ConfigurationMerger.ConfigHolder<Long, Double> configHolder2 = merger.mergeConfigurations("doubleCache", doubleCacheConfiguration);

    assertDefaultCopier(configHolder2.cacheConfiguration.getServiceConfigurations());

    MutableConfiguration<Long, Character> charCacheConfiguration  = new MutableConfiguration<Long, Character>();
    charCacheConfiguration.setTypes(Long.class, Character.class);
    ConfigurationMerger.ConfigHolder<Long, Character> configHolder3 = merger.mergeConfigurations("charCache", charCacheConfiguration);

    assertDefaultCopier(configHolder3.cacheConfiguration.getServiceConfigurations());

    MutableConfiguration<Long, Float> floatCacheConfiguration  = new MutableConfiguration<Long, Float>();
    floatCacheConfiguration.setTypes(Long.class, Float.class);
    ConfigurationMerger.ConfigHolder<Long, Float> configHolder4 = merger.mergeConfigurations("floatCache", floatCacheConfiguration);

    assertDefaultCopier(configHolder4.cacheConfiguration.getServiceConfigurations());

    MutableConfiguration<Long, Integer> integerCacheConfiguration  = new MutableConfiguration<Long, Integer>();
    integerCacheConfiguration.setTypes(Long.class, Integer.class);
    ConfigurationMerger.ConfigHolder<Long, Integer> configHolder5 = merger.mergeConfigurations("integerCache", integerCacheConfiguration);

    assertDefaultCopier(configHolder5.cacheConfiguration.getServiceConfigurations());

  }

  @Test
  public void jsr107DefaultEh107IdentityCopierForImmutableTypesWithCMLevelDefaults() {
    XmlConfiguration xmlConfiguration = new XmlConfiguration(getClass().getResource("/ehcache-107-immutable-types-cm-level-copiers.xml"));
    final DefaultJsr107Service jsr107Service = new DefaultJsr107Service(ServiceLocator.findSingletonAmongst(Jsr107Configuration.class, xmlConfiguration.getServiceCreationConfigurations().toArray()));
    merger = new ConfigurationMerger(xmlConfiguration, jsr107Service, mock(Eh107CacheLoaderWriterProvider.class));

    MutableConfiguration<Long, String> stringCacheConfiguration  = new MutableConfiguration<Long, String>();
    stringCacheConfiguration.setTypes(Long.class, String.class);
    ConfigurationMerger.ConfigHolder<Long, String> configHolder1 = merger.mergeConfigurations("stringCache", stringCacheConfiguration);

    assertThat(configHolder1.cacheConfiguration.getServiceConfigurations().isEmpty(), is(true));

    for (ServiceCreationConfiguration<?> serviceCreationConfiguration : xmlConfiguration.getServiceCreationConfigurations()) {
      if (serviceCreationConfiguration instanceof DefaultCopyProviderConfiguration) {
        DefaultCopyProviderConfiguration copierConfig = (DefaultCopyProviderConfiguration)serviceCreationConfiguration;
        assertThat(copierConfig.getDefaults().size(), is(6));
        assertThat(copierConfig.getDefaults().get(Long.class).getClazz().isAssignableFrom(IdentityCopier.class), is(true));
        assertThat(copierConfig.getDefaults().get(String.class).getClazz().isAssignableFrom(Eh107IdentityCopier.class), is(true));
        assertThat(copierConfig.getDefaults().get(Float.class).getClazz().isAssignableFrom(Eh107IdentityCopier.class), is(true));
        assertThat(copierConfig.getDefaults().get(Double.class).getClazz().isAssignableFrom(Eh107IdentityCopier.class), is(true));
        assertThat(copierConfig.getDefaults().get(Character.class).getClazz().isAssignableFrom(Eh107IdentityCopier.class), is(true));
        assertThat(copierConfig.getDefaults().get(Integer.class).getClazz().isAssignableFrom(Eh107IdentityCopier.class), is(true));
      }
    }
  }

  @Test
  public void jsr107DefaultEh107IdentityCopierForImmutableTypesWithoutTemplates() {
    MutableConfiguration<Long, String> stringCacheConfiguration  = new MutableConfiguration<Long, String>();
    stringCacheConfiguration.setTypes(Long.class, String.class);
    ConfigurationMerger.ConfigHolder<Long, String> configHolder1 = merger.mergeConfigurations("stringCache", stringCacheConfiguration);

    assertDefaultCopier(configHolder1.cacheConfiguration.getServiceConfigurations());
  }

  private static void assertDefaultCopier(Collection<ServiceConfiguration<?>> serviceConfigurations) {
    boolean noCopierConfigPresent = false;
    for (ServiceConfiguration<?> serviceConfiguration : serviceConfigurations) {
      if (serviceConfiguration instanceof DefaultCopierConfiguration) {
        noCopierConfigPresent = true;
        DefaultCopierConfiguration copierConfig = (DefaultCopierConfiguration)serviceConfiguration;
        assertThat(copierConfig.getClazz().isAssignableFrom(Eh107IdentityCopier.class), is(true));
      }
    }
    if (!noCopierConfigPresent) {
      fail();
    }
  }

  private <T> Factory<T> throwingFactory() {
    return new Factory<T>() {
      @Override
      public T create() {
        throw new UnsupportedOperationException("Boom");
      }
    };
  }

  private <T> RecordingFactory<T> factoryOf(final T instance) {
    return new RecordingFactory<T>(instance);
  }

  private static class RecordingFactory<T> implements Factory<T> {
    private final T instance;
    boolean called;

    RecordingFactory(T instance) {
      this.instance = instance;
    }

    @Override
    public T create() {
      called = true;
      return instance;
    }
  }

  private static class ThrowingCacheEntryListenerConfiguration implements CacheEntryListenerConfiguration<Object, Object> {
    @Override
    public Factory<CacheEntryListener<? super Object, ? super Object>> getCacheEntryListenerFactory() {
      throw new UnsupportedOperationException("BOOM");
    }

    @Override
    public boolean isOldValueRequired() {
      throw new UnsupportedOperationException("BOOM");
    }

    @Override
    public Factory<CacheEntryEventFilter<? super Object, ? super Object>> getCacheEntryEventFilterFactory() {
      throw new UnsupportedOperationException("BOOM");
    }

    @Override
    public boolean isSynchronous() {
      throw new UnsupportedOperationException("BOOM");
    }
  }
}