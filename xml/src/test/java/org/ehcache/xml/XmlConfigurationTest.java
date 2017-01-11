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
import org.ehcache.config.ResourceType;
import org.ehcache.config.ResourceUnit;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.internal.util.ClassLoading;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.impl.config.copy.DefaultCopierConfiguration;
import org.ehcache.impl.config.copy.DefaultCopyProviderConfiguration;
import org.ehcache.impl.config.event.DefaultCacheEventListenerConfiguration;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration;
import org.ehcache.impl.config.executor.PooledExecutionServiceConfiguration.PoolConfiguration;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.impl.config.serializer.DefaultSerializerConfiguration;
import org.ehcache.impl.config.store.disk.OffHeapDiskStoreConfiguration;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineConfiguration;
import org.ehcache.impl.config.store.heap.DefaultSizeOfEngineProviderConfiguration;
import org.ehcache.impl.copy.SerializingCopier;
import org.ehcache.spi.copy.Copier;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration;
import org.ehcache.spi.loaderwriter.WriteBehindConfiguration.BatchingConfiguration;
import org.ehcache.spi.serialization.Serializer;
import org.ehcache.spi.service.ServiceConfiguration;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXParseException;

import com.pany.ehcache.copier.AnotherPersonCopier;
import com.pany.ehcache.copier.Description;
import com.pany.ehcache.copier.DescriptionCopier;
import com.pany.ehcache.copier.Person;
import com.pany.ehcache.copier.PersonCopier;
import com.pany.ehcache.serializer.TestSerializer;
import com.pany.ehcache.serializer.TestSerializer2;
import com.pany.ehcache.serializer.TestSerializer3;
import com.pany.ehcache.serializer.TestSerializer4;

import java.io.File;
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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.ehcache.core.internal.service.ServiceLocator.findSingletonAmongst;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

/**
 *
 * @author Chris Dennis
 */
public class XmlConfigurationTest {

  @Rule
  public ExpectedException thrown= ExpectedException.none();

  @Test
  public void testDefaultTypesConfig() throws Exception {
    XmlConfiguration xmlConfig = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/defaultTypes-cache.xml"));

    assertThat(xmlConfig.getCacheConfigurations().keySet(), hasItem("foo"));
    assertThat(xmlConfig.getCacheConfigurations().get("foo").getKeyType(), sameInstance((Class)Object.class));
    assertThat(xmlConfig.getCacheConfigurations().get("foo").getValueType(), sameInstance((Class)Object.class));

    assertThat(xmlConfig.getCacheConfigurations().keySet(), hasItem("bar"));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getKeyType(), sameInstance((Class)Number.class));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getValueType(), sameInstance((Class)Object.class));

    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("example", Object.class, Object.class, heap(10)), notNullValue());

    //Allow the key/value to be assignable for xml configuration in case of type definition in template class
    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("example", Number.class, Object.class, heap(10)), notNullValue());
  }

  @Test
  public void testNonExistentAdvisorClassInCacheThrowsException() throws Exception {
    try {
      new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/nonExistentAdvisor-cache.xml"));
      fail();
    } catch (XmlConfigurationException xce) {
      assertThat(xce.getCause(), instanceOf(ClassNotFoundException.class));
    }
  }

  @Test
  public void testNonExistentAdvisorClassInTemplateThrowsException() throws Exception {
    try {
      new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/nonExistentAdvisor-template.xml"));
      fail();
    } catch (XmlConfigurationException xce) {
      assertThat(xce.getCause(), instanceOf(ClassNotFoundException.class));
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

  @Test
  public void testOneCacheConfigWithTemplate() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/template-cache.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    assertThat(xmlConfig.getServiceCreationConfigurations(), hasSize(0));
    assertThat(xmlConfig.getCacheConfigurations().keySet(), hasItem("bar"));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getServiceConfigurations(), IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(FooConfiguration.class)));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getKeyType(), sameInstance((Class) Number.class));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getValueType(), sameInstance((Class)String.class));

    final CacheConfigurationBuilder<String, String> example = xmlConfig.newCacheConfigurationBuilderFromTemplate("example", String.class, String.class,
        newResourcePoolsBuilder().heap(5, EntryUnit.ENTRIES));
    assertThat(example.build().getExpiry(),
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

    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("bar", Object.class, Object.class), nullValue());
  }

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
    } catch (XmlConfigurationException xce) {
      SAXParseException e = (SAXParseException) xce.getCause();
      assertThat(e.getLineNumber(), is(5));
      assertThat(e.getColumnNumber(), is(29));
    }
  }

  @Test
  public void testInvalidServiceConfiguration() throws Exception {
    try {
      new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/invalid-service.xml"));
    } catch (XmlConfigurationException xce) {
      SAXParseException e = (SAXParseException) xce.getCause();
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

    CacheConfigurationBuilder<String, String> tieredResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("tieredResourceTemplate", String.class, String.class);
    assertThat(tieredResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(5L));
    assertThat(tieredResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), equalTo(50L));
    assertThat(tieredResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent(), is(false));

    CacheConfigurationBuilder<String, String> persistentTieredResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("persistentTieredResourceTemplate", String.class, String.class);
    assertThat(persistentTieredResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(5L));
    assertThat(persistentTieredResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), equalTo(50L));
    assertThat(persistentTieredResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.DISK).isPersistent(), is(true));

    CacheConfigurationBuilder<String, String> tieredOffHeapResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("tieredOffHeapResourceTemplate", String.class, String.class);
    assertThat(tieredOffHeapResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(5L));
    assertThat(tieredOffHeapResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getSize(), equalTo(50L));
    assertThat(tieredOffHeapResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.OFFHEAP).getUnit(), equalTo((ResourceUnit)MemoryUnit.MB));

    CacheConfigurationBuilder<String, String> explicitHeapResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("explicitHeapResourceTemplate", String.class, String.class);
    assertThat(explicitHeapResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(15L));
    assertThat(explicitHeapResourceTemplate.build().getResourcePools().getPoolForResource(ResourceType.Core.DISK), is(nullValue()));

    CacheConfiguration<?, ?> tieredCacheConfig = xmlConfig.getCacheConfigurations().get("templatedTieredResource");
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(5L));
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK).getSize(), equalTo(50L));

    CacheConfiguration<?, ?> explicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("templatedExplicitHeapResource");
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(15L));
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.DISK), is(nullValue()));
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
  public void testThreadPoolsConfiguration() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/thread-pools.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    assertThat(xmlConfig.getServiceCreationConfigurations(), contains(instanceOf(PooledExecutionServiceConfiguration.class)));

    PooledExecutionServiceConfiguration configuration = (PooledExecutionServiceConfiguration) xmlConfig.getServiceCreationConfigurations().iterator().next();

    assertThat(configuration.getPoolConfigurations().keySet(), containsInAnyOrder("big", "small"));

    PoolConfiguration small = configuration.getPoolConfigurations().get("small");
    assertThat(small.minSize(), is(1));
    assertThat(small.maxSize(), is(1));

    PoolConfiguration big = configuration.getPoolConfigurations().get("big");
    assertThat(big.minSize(), is(4));
    assertThat(big.maxSize(), is(32));

    assertThat(configuration.getDefaultPoolAlias(), is("big"));
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
        if(copierConfig.getType() == DefaultCopierConfiguration.Type.KEY) {
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
        if(copierConfig.getType() == DefaultCopierConfiguration.Type.KEY) {
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
    assertThat(serviceConfig, instanceOf(DefaultPersistenceConfiguration.class));

    DefaultPersistenceConfiguration persistenceConfiguration = (DefaultPersistenceConfiguration)serviceConfig;
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

    serviceConfiguration = xmlConfig.newCacheConfigurationBuilderFromTemplate("example", Number.class, String.class).build().getServiceConfigurations();

    assertThat(serviceConfiguration, IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(WriteBehindConfiguration.class)));

    for (ServiceConfiguration<?> configuration : serviceConfiguration) {
      if(configuration instanceof WriteBehindConfiguration) {
        BatchingConfiguration batchingConfig = ((WriteBehindConfiguration) configuration).getBatchingConfiguration();
        assertThat(batchingConfig.getMaxDelay(), is(10L));
        assertThat(batchingConfig.getMaxDelayUnit(), is(SECONDS));
        assertThat(batchingConfig.isCoalescing(), is(false));
        assertThat(batchingConfig.getBatchSize(), is(2));
        assertThat(((WriteBehindConfiguration) configuration).getConcurrency(), is(1));
        assertThat(((WriteBehindConfiguration) configuration).getMaxQueueSize(), is(10));
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

    CacheConfigurationBuilder<Number, String> templateConfig = xmlConfig.newCacheConfigurationBuilderFromTemplate("example", Number.class, String.class);
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

  @Test
  public void testDiskStoreSettings() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/resources-caches.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    CacheConfiguration<?, ?> cacheConfig = xmlConfig.getCacheConfigurations().get("tiered");

    OffHeapDiskStoreConfiguration diskConfig = findSingletonAmongst(OffHeapDiskStoreConfiguration.class, cacheConfig.getServiceConfigurations().toArray());

    assertThat(diskConfig.getThreadPoolAlias(), is("some-pool"));
    assertThat(diskConfig.getWriterConcurrency(), is(2));
  }

  @Test
  public void testNullUrlInConstructorThrowsNPE() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("The url can not be null");
    XmlConfiguration xmlConfig = new XmlConfiguration(null, mock(ClassLoader.class), getClassLoaderMapMock());
  }

  @Test
  public void testNullClassLoaderInConstructorThrowsNPE() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("The classLoader can not be null");
    XmlConfiguration xmlConfig = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"), null, getClassLoaderMapMock());
  }

  @Test
  public void testNullCacheClassLoaderMapInConstructorThrowsNPE() throws Exception {
    thrown.expect(NullPointerException.class);
    thrown.expectMessage("The cacheClassLoaders map can not be null");
    XmlConfiguration xmlConfig = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"), mock(ClassLoader.class), null);
  }

  @Test
  public void testSizeOfEngineLimits() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/sizeof-engine.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);
    DefaultSizeOfEngineProviderConfiguration sizeOfEngineProviderConfig = findSingletonAmongst(DefaultSizeOfEngineProviderConfiguration.class, xmlConfig.getServiceCreationConfigurations());

    assertThat(sizeOfEngineProviderConfig, notNullValue());
    assertEquals(sizeOfEngineProviderConfig.getMaxObjectGraphSize(), 200);
    assertEquals(sizeOfEngineProviderConfig.getMaxObjectSize(), 100000);

    CacheConfiguration<?, ?> cacheConfig = xmlConfig.getCacheConfigurations().get("usesDefaultSizeOfEngine");
    DefaultSizeOfEngineConfiguration sizeOfEngineConfig = findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, cacheConfig.getServiceConfigurations());

    assertThat(sizeOfEngineConfig, nullValue());

    CacheConfiguration<?, ?> cacheConfig1 = xmlConfig.getCacheConfigurations().get("usesConfiguredInCache");
    DefaultSizeOfEngineConfiguration sizeOfEngineConfig1 = findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, cacheConfig1.getServiceConfigurations());

    assertThat(sizeOfEngineConfig1, notNullValue());
    assertEquals(sizeOfEngineConfig1.getMaxObjectGraphSize(), 500);
    assertEquals(sizeOfEngineConfig1.getMaxObjectSize(), 200000);

    CacheConfiguration<?, ?> cacheConfig2 = xmlConfig.getCacheConfigurations().get("usesPartialOneConfiguredInCache");
    DefaultSizeOfEngineConfiguration sizeOfEngineConfig2 = findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, cacheConfig2.getServiceConfigurations());

    assertThat(sizeOfEngineConfig2, notNullValue());
    assertThat(sizeOfEngineConfig2.getMaxObjectGraphSize(), is(500L));
    assertThat(sizeOfEngineConfig2.getMaxObjectSize(), is(Long.MAX_VALUE));

    CacheConfiguration<?, ?> cacheConfig3 = xmlConfig.getCacheConfigurations().get("usesPartialTwoConfiguredInCache");
    DefaultSizeOfEngineConfiguration sizeOfEngineConfig3 = findSingletonAmongst(DefaultSizeOfEngineConfiguration.class, cacheConfig3.getServiceConfigurations());

    assertThat(sizeOfEngineConfig3, notNullValue());
    assertThat(sizeOfEngineConfig3.getMaxObjectGraphSize(), is(1000L));
    assertThat(sizeOfEngineConfig3.getMaxObjectSize(), is(200000L));
  }

  @Test
  public void testCacheManagerDefaultObjectGraphSize() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/sizeof-engine-cm-defaults-one.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);
    DefaultSizeOfEngineProviderConfiguration sizeOfEngineProviderConfig = findSingletonAmongst(DefaultSizeOfEngineProviderConfiguration.class, xmlConfig.getServiceCreationConfigurations());

    assertThat(sizeOfEngineProviderConfig, notNullValue());
    assertThat(sizeOfEngineProviderConfig.getMaxObjectGraphSize(), is(1000L));
    assertThat(sizeOfEngineProviderConfig.getMaxObjectSize(), is(100000L));
  }

  @Test
  public void testCacheManagerDefaultObjectSize() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/sizeof-engine-cm-defaults-two.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);
    DefaultSizeOfEngineProviderConfiguration sizeOfEngineProviderConfig = findSingletonAmongst(DefaultSizeOfEngineProviderConfiguration.class, xmlConfig.getServiceCreationConfigurations());

    assertThat(sizeOfEngineProviderConfig, notNullValue());
    assertThat(sizeOfEngineProviderConfig.getMaxObjectGraphSize(), is(200L));
    assertThat(sizeOfEngineProviderConfig.getMaxObjectSize(), is(Long.MAX_VALUE));
  }

  @Test
  public void testCustomResource() throws Exception {
    try {
      new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/custom-resource.xml"));
    } catch (XmlConfigurationException xce) {
      assertThat(xce.getMessage(), containsString("Can't find parser for namespace: http://www.example.com/fancy"));
    }
  }

  @Test
  public void testSysPropReplace() {
    System.getProperties().setProperty("ehcache.match", Number.class.getName());
    XmlConfiguration xmlConfig = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/systemprops.xml"));

    assertThat(xmlConfig.getCacheConfigurations().get("bar").getKeyType(), sameInstance((Class)Number.class));

    DefaultPersistenceConfiguration persistenceConfiguration = (DefaultPersistenceConfiguration)xmlConfig.getServiceCreationConfigurations().iterator().next();
    assertThat(persistenceConfiguration.getRootDirectory(), is(new File(System.getProperty("user.home") + "/ehcache")));
  }

  @Test
  public void testSysPropReplaceRegExp() {
    assertThat(ConfigurationParser.replaceProperties("foo${file.separator}", System.getProperties()), equalTo("foo" + File.separator));
    assertThat(ConfigurationParser.replaceProperties("${file.separator}foo${file.separator}", System.getProperties()), equalTo(File.separator + "foo" + File.separator));
    try {
      ConfigurationParser.replaceProperties("${bar}foo", System.getProperties());
      fail("Should have thrown!");
    } catch (IllegalStateException e) {
      assertThat(e.getMessage().contains("${bar}"), is(true));
    }
    assertThat(ConfigurationParser.replaceProperties("foo", System.getProperties()), nullValue());
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

  @SuppressWarnings("unchecked")
  private Map<String, ClassLoader> getClassLoaderMapMock() {
    return (Map<String, ClassLoader>) mock(Map.class);
  }
}
