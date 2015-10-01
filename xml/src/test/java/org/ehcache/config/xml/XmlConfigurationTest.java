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

import com.pany.ehcache.copier.AnotherPersonCopier;
import com.pany.ehcache.copier.Description;
import com.pany.ehcache.copier.DescriptionCopier;
import com.pany.ehcache.copier.Person;
import com.pany.ehcache.copier.PersonCopier;
import com.pany.ehcache.serializer.TestSerializer;
import com.pany.ehcache.serializer.TestSerializer2;
import com.pany.ehcache.serializer.TestSerializer3;
import com.pany.ehcache.serializer.TestSerializer4;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.Configuration;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.copy.CopierConfiguration;
import org.ehcache.config.copy.DefaultCopierConfiguration;
import org.ehcache.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.config.persistence.PersistenceConfiguration;
import org.ehcache.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.config.writebehind.DefaultWriteBehindConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.copy.SerializingCopier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import org.w3c.dom.Document;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.ehcache.util.ClassLoading;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringContains.containsString;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import org.w3c.dom.Element;

/**
 *
 * @author Chris Dennis
 */
public class XmlConfigurationTest {

  @SuppressWarnings("rawtypes")
  @Test
  public void testDefaultTypesConfig() throws Exception {
    XmlConfiguration xmlConfig = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/defaultTypes-cache.xml"));

    assertThat(xmlConfig.getCacheConfigurations().keySet(), hasItem("foo"));
    assertThat(xmlConfig.getCacheConfigurations().get("foo").getKeyType(), sameInstance((Class)Object.class));
    assertThat(xmlConfig.getCacheConfigurations().get("foo").getValueType(), sameInstance((Class)Object.class));

    assertThat(xmlConfig.getCacheConfigurations().keySet(), hasItem("bar"));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getKeyType(), sameInstance((Class)Number.class));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getValueType(), sameInstance((Class)Object.class));

    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("example"), notNullValue());
    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("example", Object.class, Object.class), notNullValue());

    //Allow the key/value to be assignable for xml configuration in case of type definition in template class
    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("example", Number.class, Object.class), notNullValue());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testPrioritizerCache() throws Exception {
    XmlConfiguration xmlConfig = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/prioritizer-cache.xml"));

    assertThat(xmlConfig.getCacheConfigurations().keySet(), hasItem("foo"));
    assertThat(xmlConfig.getCacheConfigurations().get("foo").getEvictionPrioritizer(), sameInstance((EvictionPrioritizer)Eviction.Prioritizer.LFU));

    CacheConfigurationBuilder<Object, Object> example = xmlConfig.newCacheConfigurationBuilderFromTemplate("example");
    assertThat(example.buildConfig(Object.class, Object.class).getEvictionPrioritizer(), sameInstance((EvictionPrioritizer) Eviction.Prioritizer.LFU));
  }

  @Test
  public void testNonExistentVetoClassInCacheThrowsException() throws Exception {
    try {
      new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/nonExistentVeto-cache.xml"));
      fail();
    } catch (ClassNotFoundException cnfe) {
      // expected
    }
  }

  @Test
  public void testNonExistentVetoClassInTemplateThrowsException() throws Exception {
    try {
      new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/nonExistentVeto-template.xml"));
      fail();
    } catch (ClassNotFoundException cnfe) {
      // expected
    }
  }

  @Test
  public void testOneServiceConfig() throws Exception {
    Configuration config = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-service.xml"));

    assertThat(config.getServiceCreationConfigurations(), IsCollectionContaining.<ServiceCreationConfiguration<?>>hasItem(instanceOf(BarConfiguration.class)));
    assertThat(config.getCacheConfigurations().keySet(), hasSize(0));
  }

  @Test
  public void testOneCacheConfig() throws Exception {
    Configuration config = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"));

    assertThat(config.getServiceCreationConfigurations(), hasSize(0));
    assertThat(config.getCacheConfigurations().keySet(), hasItem("bar"));
    assertThat(config.getCacheConfigurations().get("bar").getServiceConfigurations(), IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(FooConfiguration.class)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testOneCacheConfigWithTemplate() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/template-cache.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    assertThat(xmlConfig.getServiceCreationConfigurations(), hasSize(0));
    assertThat(xmlConfig.getCacheConfigurations().keySet(), hasItem("bar"));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getServiceConfigurations(), IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(FooConfiguration.class)));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getKeyType(), sameInstance((Class) Number.class));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getValueType(), sameInstance((Class)String.class));

    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("example"), notNullValue());
    final CacheConfigurationBuilder<String, String> example = xmlConfig.newCacheConfigurationBuilderFromTemplate("example", String.class, String.class);
    assertThat(example.buildConfig(String.class, String.class).getExpiry(),
        equalTo((Expiry) Expirations.timeToLiveExpiration(new Duration(30, TimeUnit.SECONDS))));

    try {
      xmlConfig.newCacheConfigurationBuilderFromTemplate("example", String.class, Number.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("CacheTemplate 'example' declares value type of java.lang.String"));
    }
    try {
      xmlConfig.newCacheConfigurationBuilderFromTemplate("example", Number.class, String.class);
      fail();
    } catch (IllegalArgumentException e) {
      assertThat(e.getMessage(), is("CacheTemplate 'example' declares key type of java.lang.String"));
    }

    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("bar"), nullValue());
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testEvictionPrioritizer() throws ClassNotFoundException, SAXException, InstantiationException, IOException, IllegalAccessException {
    final XmlConfiguration xmlConfiguration = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/cache-eviction.xml"));
    final EvictionPrioritizer lru = xmlConfiguration.getCacheConfigurations().get("lru").getEvictionPrioritizer();
    final EvictionPrioritizer value = Eviction.Prioritizer.FIFO;
    assertThat(lru, is(value));
    final EvictionPrioritizer mine = xmlConfiguration.getCacheConfigurations().get("eviction").getEvictionPrioritizer();
    assertThat(mine, CoreMatchers.instanceOf(com.pany.ehcache.MyEviction.class));

  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testExpiryIsParsed() throws Exception {
    final XmlConfiguration xmlConfiguration = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/expiry-caches.xml"));

    Expiry expiry = xmlConfiguration.getCacheConfigurations().get("none").getExpiry();
    Expiry value = Expirations.noExpiration();
    assertThat(expiry, is(value));

    expiry = xmlConfiguration.getCacheConfigurations().get("notSet").getExpiry();
    value = Expirations.noExpiration();
    assertThat(expiry, is(value));

    expiry = xmlConfiguration.getCacheConfigurations().get("class").getExpiry();
    assertThat(expiry, CoreMatchers.instanceOf(com.pany.ehcache.MyExpiry.class));

    expiry = xmlConfiguration.getCacheConfigurations().get("tti").getExpiry();
    value = Expirations.timeToIdleExpiration(new Duration(500, TimeUnit.MILLISECONDS));
    assertThat(expiry, equalTo(value));

    expiry = xmlConfiguration.getCacheConfigurations().get("ttl").getExpiry();
    value = Expirations.timeToLiveExpiration(new Duration(30, TimeUnit.SECONDS));
    assertThat(expiry, equalTo(value));
  }

  @Test
  public void testInvalidCoreConfiguration() throws Exception {
    try {
      new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/invalid-core.xml"));
      fail();
    } catch (SAXParseException e) {
      assertThat(e.getLineNumber(), is(5));
      assertThat(e.getColumnNumber(), is(29));
    }
  }

  @Test
  public void testInvalidServiceConfiguration() throws Exception {
    try {
      new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/invalid-service.xml"));
    } catch (SAXParseException e) {
      assertThat(e.getLineNumber(), is(6));
      assertThat(e.getColumnNumber(), is(15));
    }
  }

  @Test
  public void testExposesProperURL() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/one-cache.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    assertThat(xmlConfig.getURL(), equalTo(resource));
  }

  @Test
  public void testResourcesCaches() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/resources-caches.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    CacheConfiguration<?, ?> tieredCacheConfig = xmlConfig.getCacheConfigurations().get("tiered");
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(10L));
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), equalTo(100L));
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent(), is(false));

    CacheConfiguration<?, ?> tieredPersistentCacheConfig = xmlConfig.getCacheConfigurations().get("tieredPersistent");
    assertThat(tieredPersistentCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(10L));
    assertThat(tieredPersistentCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), equalTo(100L));
    assertThat(tieredPersistentCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent(), is(true));

    CacheConfiguration<?, ?> tieredOffHeapCacheConfig = xmlConfig.getCacheConfigurations().get("tieredOffHeap");
    assertThat(tieredOffHeapCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(10L));
    assertThat(tieredOffHeapCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getSize(), equalTo(10L));
    assertThat(tieredOffHeapCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getUnit(), equalTo((ResourceUnit) MemoryUnit.MB));

    CacheConfiguration<?, ?> explicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("explicitHeapOnly");
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(15L));
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK), is(nullValue()));

    CacheConfiguration<?, ?> implicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("directHeapOnly");
    assertThat(implicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(25L));
    assertThat(implicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK), is(nullValue()));
  }

  @Test
  public void testResourcesTemplates() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/resources-templates.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    CacheConfigurationBuilder<Object, Object> tieredResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("tieredResourceTemplate");
    assertThat(tieredResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(5L));
    assertThat(tieredResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), equalTo(50L));
    assertThat(tieredResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent(), is(false));

    CacheConfigurationBuilder<Object, Object> persistentTieredResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("persistentTieredResourceTemplate");
    assertThat(persistentTieredResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(5L));
    assertThat(persistentTieredResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), equalTo(50L));
    assertThat(persistentTieredResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent(), is(true));

    CacheConfigurationBuilder<Object, Object> tieredOffHeapResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("tieredOffHeapResourceTemplate");
    assertThat(tieredOffHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(5L));
    assertThat(tieredOffHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getSize(), equalTo(50L));
    assertThat(tieredOffHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getUnit(), equalTo((ResourceUnit)MemoryUnit.MB));

    CacheConfigurationBuilder<Object, Object> explicitHeapResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("explicitHeapResourceTemplate");
    assertThat(explicitHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(15L));
    assertThat(explicitHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.DISK), is(nullValue()));

    CacheConfigurationBuilder<Object, Object> implicitHeapResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("implicitHeapResourceTemplate");
    assertThat(implicitHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.HEAP), is(nullValue()));
    assertThat(implicitHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource(ResourceType.Core.DISK), is(nullValue()));

    CacheConfiguration<?, ?> tieredCacheConfig = xmlConfig.getCacheConfigurations().get("templatedTieredResource");
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(5L));
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), equalTo(50L));

    CacheConfiguration<?, ?> explicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("templatedExplicitHeapResource");
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(15L));
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK), is(nullValue()));

    CacheConfiguration<?, ?> implicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("templatedImplicitHeapResource");
    assertThat(implicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP), is(nullValue()));
    assertThat(implicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK), is(nullValue()));
  }

  @Test
  public void testNoClassLoaderSpecified() throws Exception {
    XmlConfiguration config = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"));

    assertSame(config.getClassLoader(), ClassLoading.getDefaultClassLoader());
    assertNull(config.getCacheConfigurations().get("bar").getClassLoader());
  }
  
  @Test
  public void testClassLoaderSpecified() throws Exception {
    ClassLoader cl = new ClassLoader() {
      //
    };
    
    XmlConfiguration config= new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"), cl);

    assertSame(cl, config.getClassLoader());
    assertNull(config.getCacheConfigurations().get("bar").getClassLoader());
  }
  
  @Test
  public void testCacheClassLoaderSpecified() throws Exception {
    ClassLoader cl = new ClassLoader() {
      //
    };
    
    ClassLoader cl2 = new ClassLoader() {
      //
    };
    
    assertNotSame(cl, cl2);

    Map<String, ClassLoader> loaders = new HashMap<String, ClassLoader>();
    loaders.put("bar", cl2);
    XmlConfiguration config = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"), cl, loaders);

    assertSame(cl, config.getClassLoader());
    assertSame(cl2, config.getCacheConfigurations().get("bar").getClassLoader());
  }

  @Test
  public void testDefaultSerializerConfiguration() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/default-serializer.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    assertThat(xmlConfig.getServiceCreationConfigurations().size(), is(1));

    ServiceCreationConfiguration configuration = xmlConfig.getServiceCreationConfigurations().iterator().next();

    assertThat(configuration, instanceOf(DefaultSerializationProviderConfiguration.class));

    DefaultSerializationProviderConfiguration factoryConfiguration = (DefaultSerializationProviderConfiguration) configuration;
    assertThat(factoryConfiguration.getTransientSerializers().size(), is(4));
    assertThat(factoryConfiguration.getTransientSerializers().get(CharSequence.class), Matchers.<Class<? extends Serializer>>equalTo(TestSerializer.class));
    assertThat(factoryConfiguration.getTransientSerializers().get(Number.class), Matchers.<Class<? extends Serializer>>equalTo(TestSerializer2.class));
    assertThat(factoryConfiguration.getTransientSerializers().get(Long.class), Matchers.<Class<? extends Serializer>>equalTo(TestSerializer3.class));
    assertThat(factoryConfiguration.getTransientSerializers().get(Integer.class), Matchers.<Class<? extends Serializer>>equalTo(TestSerializer4.class));


    List<ServiceConfiguration<?>> orderedServiceConfigurations = new ArrayList<ServiceConfiguration<?>>(xmlConfig.getCacheConfigurations().get("baz").getServiceConfigurations());
    // order services by class name so the test can rely on some sort of ordering
    Collections.sort(orderedServiceConfigurations, new Comparator<ServiceConfiguration<?>>() {
      @Override
      public int compare(ServiceConfiguration<?> o1, ServiceConfiguration<?> o2) {
        return o1.getClass().getName().compareTo(o2.getClass().getName());
      }
    });
    Iterator<ServiceConfiguration<?>> it = orderedServiceConfigurations.iterator();

    DefaultSerializerConfiguration keySerializationProviderConfiguration = (DefaultSerializerConfiguration) it.next();
    assertThat(keySerializationProviderConfiguration.getType(), isIn(DefaultSerializerConfiguration.Type.KEY, DefaultSerializerConfiguration.Type.VALUE));
  }

  public static <T> Matcher<T> isIn(T... elements) {
    return org.hamcrest.collection.IsIn.isIn(elements);
  }

  @Test
  public void testCacheCopierConfiguration() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/cache-copiers.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    assertThat(xmlConfig.getServiceCreationConfigurations().size(), is(1));

    ServiceCreationConfiguration configuration = xmlConfig.getServiceCreationConfigurations().iterator().next();

    assertThat(configuration, instanceOf(DefaultCopyProviderConfiguration.class));

    DefaultCopyProviderConfiguration factoryConfiguration = (DefaultCopyProviderConfiguration) configuration;
    assertThat(factoryConfiguration.getDefaults().size(), is(2));
    assertThat(factoryConfiguration.getDefaults().get(Description.class).getClazz(),
        Matchers.<Class<? extends Copier>>equalTo(DescriptionCopier.class));
    assertThat(factoryConfiguration.getDefaults().get(Person.class).getClazz(),
        Matchers.<Class<? extends Copier>>equalTo(PersonCopier.class));


    Collection<ServiceConfiguration<?>> configs = xmlConfig.getCacheConfigurations().get("baz").getServiceConfigurations();
    for(ServiceConfiguration<?> config: configs) {
      if(config instanceof DefaultCopierConfiguration) {
        DefaultCopierConfiguration copierConfig = (DefaultCopierConfiguration) config;
        if(copierConfig.getType() == CopierConfiguration.Type.KEY) {
          assertEquals(SerializingCopier.class, copierConfig.getClazz());
        } else {
          assertEquals(AnotherPersonCopier.class, copierConfig.getClazz());
        }
      } else {
        continue;
      }
    }

    configs = xmlConfig.getCacheConfigurations().get("bak").getServiceConfigurations();
    for(ServiceConfiguration<?> config: configs) {
      if(config instanceof DefaultCopierConfiguration) {
        DefaultCopierConfiguration copierConfig = (DefaultCopierConfiguration) config;
        if(copierConfig.getType() == CopierConfiguration.Type.KEY) {
          assertEquals(SerializingCopier.class, copierConfig.getClazz());
        } else {
          assertEquals(AnotherPersonCopier.class, copierConfig.getClazz());
        }
      } else {
        continue;
      }
    }
  }

  @Test
  public void testPersistenceConfig() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/persistence-config.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    ServiceCreationConfiguration<?> serviceConfig = xmlConfig.getServiceCreationConfigurations().iterator().next();
    assertThat(serviceConfig, instanceOf(PersistenceConfiguration.class));

    PersistenceConfiguration persistenceConfiguration = (PersistenceConfiguration)serviceConfig;
    assertThat(persistenceConfiguration.getRootDirectory(), is(new File("   \n\t/my/caching/persistence  directory\r\n      ")));
  }

  @Test
  public void testPersistenceConfigXmlPersistencePathHasWhitespaces() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/persistence-config.xml");
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse(new File(resource.toURI()));

    Element persistence = (Element) doc.getElementsByTagName("ehcache:persistence").item(0);
    String directoryValue = persistence.getAttribute("directory");
    assertThat(directoryValue, containsString(" "));
    assertThat(directoryValue, containsString("\r"));
    assertThat(directoryValue, containsString("\n"));
    assertThat(directoryValue, containsString("\t"));
  }
  
  @Test
  public void testWriteBehind() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/writebehind-cache.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);
    
    Collection<ServiceConfiguration<?>> serviceConfiguration = xmlConfig.getCacheConfigurations().get("bar").getServiceConfigurations();
    
    assertThat(serviceConfiguration, IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(WriteBehindConfiguration.class)));
    
    serviceConfiguration = xmlConfig.newCacheConfigurationBuilderFromTemplate("example").buildConfig(Number.class, String.class).getServiceConfigurations();
    
    assertThat(serviceConfiguration, IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(WriteBehindConfiguration.class)));
    
    for (ServiceConfiguration<?> configuration : serviceConfiguration) {
      if(configuration instanceof DefaultWriteBehindConfiguration) {
        assertThat(((WriteBehindConfiguration) configuration).getMaxWriteDelay(), is(Integer.MAX_VALUE));
        assertThat(((WriteBehindConfiguration) configuration).getMinWriteDelay(), is(0));
        assertThat(((WriteBehindConfiguration) configuration).isWriteCoalescing(), is(false));
        assertThat(((WriteBehindConfiguration) configuration).getWriteBatchSize(), is(2));
        assertThat(((WriteBehindConfiguration) configuration).getWriteBehindConcurrency(), is(1));
        assertThat(((WriteBehindConfiguration) configuration).getWriteBehindMaxQueueSize(), is(10));
        assertThat(((WriteBehindConfiguration) configuration).getRateLimitPerSecond(), is(Integer.MAX_VALUE));
        assertThat(((WriteBehindConfiguration) configuration).getRetryAttempts(), is(0));
        assertThat(((WriteBehindConfiguration) configuration).getRetryAttemptDelaySeconds(), is(1));
        break;
      }
      
    }
    
  }

  @Test
  public void testCacheEventListener() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/ehcache-cacheEventListener.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);
    assertThat(xmlConfig.getCacheConfigurations().size(), is(2));

    Collection<?> configuration = xmlConfig.getCacheConfigurations().get("bar").getServiceConfigurations();
    checkListenerConfigurationExists(configuration);
  }

  @Test
  public void testCacheEventListenerThroughTemplate() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/ehcache-cacheEventListener.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);
    CacheConfiguration<?, ?> cacheConfig = xmlConfig.getCacheConfigurations().get("template1");
    checkListenerConfigurationExists(cacheConfig.getServiceConfigurations());

    CacheConfigurationBuilder<Object, Object> templateConfig = xmlConfig.newCacheConfigurationBuilderFromTemplate("example");
    assertThat(templateConfig.getExistingServiceConfiguration(DefaultCacheEventListenerConfiguration.class), notNullValue());
  }
  
  @Test
  public void testDefaulSerializerXmlsSerializersValueHasWhitespaces() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/default-serializer.xml");
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    Document doc = dBuilder.parse(new File(resource.toURI()));

    NodeList nList = doc.getElementsByTagName("ehcache:serializer");
   
    assertThat(nList.item(2).getFirstChild().getNodeValue(), containsString(" "));
    assertThat(nList.item(2).getFirstChild().getNodeValue(), containsString("\n"));
    
    assertThat(nList.item(3).getFirstChild().getNodeValue(), containsString(" "));
    assertThat(nList.item(3).getFirstChild().getNodeValue(), containsString("\n"));
    
    
    nList = doc.getElementsByTagName("ehcache:key-type");
    
    assertThat(nList.item(0).getFirstChild().getNodeValue(), containsString(" "));
    assertThat(nList.item(0).getFirstChild().getNodeValue(), containsString("\n"));
    
    assertThat(nList.item(1).getFirstChild().getNodeValue(), containsString(" "));
    assertThat(nList.item(1).getFirstChild().getNodeValue(), containsString("\n"));
    
    nList = doc.getElementsByTagName("ehcache:value-type");
    assertThat(nList.item(0).getFirstChild().getNodeValue(), containsString(" "));
    assertThat(nList.item(0).getFirstChild().getNodeValue(), containsString("\n"));
    
    assertThat(nList.item(1).getFirstChild().getNodeValue(), containsString(" "));
    assertThat(nList.item(1).getFirstChild().getNodeValue(), containsString("\n"));
    

  }

  private void checkListenerConfigurationExists(Collection<?> configuration) {
    int count = 0;
    for (Object o : configuration) {
      if(o instanceof DefaultCacheEventListenerConfiguration) {
        count++;
      }
    }
    assertThat(count, is(1));
  }

}
