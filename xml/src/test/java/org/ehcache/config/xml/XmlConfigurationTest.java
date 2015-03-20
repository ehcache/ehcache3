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
import org.ehcache.config.serializer.DefaultSerializationProviderConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.store.heap.service.OnHeapStoreServiceConfig;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.IOException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsSame.sameInstance;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

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

    assertThat(config.getServiceConfigurations(), IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(FooConfiguration.class)));
    assertThat(config.getCacheConfigurations().keySet(), hasSize(0));
  }

  @Test
  public void testOneCacheConfig() throws Exception {
    Configuration config = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"));

    assertThat(config.getServiceConfigurations(), hasSize(0));
    assertThat(config.getCacheConfigurations().keySet(), hasItem("bar"));
    assertThat(config.getCacheConfigurations().get("bar").getServiceConfigurations(), IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(FooConfiguration.class)));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testOneCacheConfigWithTemplate() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/template-cache.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    assertThat(xmlConfig.getServiceConfigurations(), hasSize(0));
    assertThat(xmlConfig.getCacheConfigurations().keySet(), hasItem("bar"));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getServiceConfigurations(), IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(FooConfiguration.class)));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getKeyType(), sameInstance((Class) Number.class));
    assertThat(xmlConfig.getCacheConfigurations().get("bar").getValueType(), sameInstance((Class)String.class));

    assertThat(xmlConfig.newCacheConfigurationBuilderFromTemplate("example"), notNullValue());
    final CacheConfigurationBuilder<String, String> example = xmlConfig.newCacheConfigurationBuilderFromTemplate("example", String.class, String.class);
    assertThat(example.buildConfig(String.class, String.class).getExpiry(),
        equalTo((Expiry)Expirations.timeToLiveExpiration(new Duration(30, TimeUnit.SECONDS))));

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

  @Test
  public void testStoreByValueDefaultsToFalse() throws Exception {
    Boolean storeByValueOnHeap = null;
    final XmlConfiguration xmlConfiguration = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"));
    for (ServiceConfiguration<?> serviceConfiguration : xmlConfiguration.getCacheConfigurations()
        .get("bar")
        .getServiceConfigurations()) {
      if(serviceConfiguration instanceof OnHeapStoreServiceConfig) {
        storeByValueOnHeap = ((OnHeapStoreServiceConfig)serviceConfiguration).storeByValue();
      }
    }
    assertThat(storeByValueOnHeap, is(false));
  }
  
  @Test
  public void testStoreByValueIsParsed() throws Exception {
    Boolean storeByValueOnHeap = null;
    final XmlConfiguration xmlConfiguration = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/byValue-cache.xml"));
    for (ServiceConfiguration<?> serviceConfiguration : xmlConfiguration.getCacheConfigurations()
        .get("bar")
        .getServiceConfigurations()) {
      if(serviceConfiguration instanceof OnHeapStoreServiceConfig) {
        storeByValueOnHeap = ((OnHeapStoreServiceConfig)serviceConfiguration).storeByValue();
      }
    }
    assertThat(storeByValueOnHeap, is(true));
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
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource("heap").getValue(), equalTo("10"));
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource("disk").getValue(), equalTo("100"));

    CacheConfiguration<?, ?> explicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("explicitHeapOnly");
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource("heap").getValue(), equalTo("15"));
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource("disk"), is(nullValue()));

    CacheConfiguration<?, ?> implicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("implicitHeapOnly");
    assertThat(implicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource("heap"), is(nullValue()));
    assertThat(implicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource("disk"), is(nullValue()));
  }

  @Test
  public void testResourcesTemplates() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/resources-templates.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);

    CacheConfigurationBuilder<Object, Object> tieredResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("tieredResourceTemplate");
    assertThat(tieredResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource("heap").getValue(), equalTo("5"));
    assertThat(tieredResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource("disk").getValue(), equalTo("50"));

    CacheConfigurationBuilder<Object, Object> explicitHeapResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("explicitHeapResourceTemplate");
    assertThat(explicitHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource("heap").getValue(), equalTo("15"));
    assertThat(explicitHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource("disk"), is(nullValue()));

    CacheConfigurationBuilder<Object, Object> implicitHeapResourceTemplate = xmlConfig.newCacheConfigurationBuilderFromTemplate("implicitHeapResourceTemplate");
    assertThat(implicitHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource("heap"), is(nullValue()));
    assertThat(implicitHeapResourceTemplate.buildConfig(String.class, String.class).getResourcePools().getPoolForResource("disk"), is(nullValue()));

    CacheConfiguration<?, ?> tieredCacheConfig = xmlConfig.getCacheConfigurations().get("templatedTieredResource");
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource("heap").getValue(), equalTo("5"));
    assertThat(tieredCacheConfig.getResourcePools().getPoolForResource("disk").getValue(), equalTo("50"));

    CacheConfiguration<?, ?> explicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("templatedExplicitHeapResource");
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource("heap").getValue(), equalTo("15"));
    assertThat(explicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource("disk"), is(nullValue()));

    CacheConfiguration<?, ?> implicitHeapOnlyCacheConfig = xmlConfig.getCacheConfigurations().get("templatedImplicitHeapResource");
    assertThat(implicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource("heap"), is(nullValue()));
    assertThat(implicitHeapOnlyCacheConfig.getResourcePools().getPoolForResource("disk"), is(nullValue()));
  }

  @Test
  public void testNoClassLoaderSpecified() throws Exception {
    XmlConfiguration config = new XmlConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"));

    assertNull(config.getClassLoader());
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
  public void testSerializerConfiguration() throws Exception {
    final URL resource = XmlConfigurationTest.class.getResource("/configs/ehcache-serializer.xml");
    XmlConfiguration xmlConfig = new XmlConfiguration(resource);
    
    assertThat(xmlConfig.getServiceConfigurations().size(), is(1));
    
    ServiceConfiguration configuration = xmlConfig.getServiceConfigurations().iterator().next();
    
    assertThat(configuration, instanceOf(DefaultSerializationProviderConfiguration.class));
    
    assertThat(((DefaultSerializationProviderConfiguration)configuration).getTypeSerializerConfig("java.lang.Number").getCacheTypeSerializerMapping().size(), is(2) );
    assertThat(((DefaultSerializationProviderConfiguration)configuration).getTypeSerializerConfig("java.lang.String").getCacheTypeSerializerMapping().size(), is(0) );
    
    
   
  }

}
