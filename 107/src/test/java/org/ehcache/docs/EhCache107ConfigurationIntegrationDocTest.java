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

package org.ehcache.docs;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.ResourceType;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.core.config.DefaultConfiguration;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.jsr107.Eh107Configuration;
import org.ehcache.jsr107.EhcacheCachingProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pany.domain.Client;
import com.pany.domain.Product;

import java.io.File;
import java.util.Random;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/**
 * This class uses unit test assertions but serves mostly as the live code repository for Asciidoctor documentation.
 */
public class EhCache107ConfigurationIntegrationDocTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(EhCache107ConfigurationIntegrationDocTest.class);

  private CacheManager cacheManager;
  private CachingProvider cachingProvider;
  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  @Before
  public void setUp() throws Exception {
    cachingProvider = Caching.getCachingProvider();
    cacheManager = cachingProvider.getCacheManager();
  }

  @After
  public void tearDown() throws Exception {
    if(cacheManager != null) {
      cacheManager.close();
    }
    if(cachingProvider != null) {
      cachingProvider.close();
    }
  }

  @Test
  public void basicConfiguration() throws Exception {
    // tag::basicConfigurationExample[]
    CachingProvider provider = Caching.getCachingProvider();  // <1>
    CacheManager cacheManager = provider.getCacheManager();   // <2>
    MutableConfiguration<Long, String> configuration =
        new MutableConfiguration<Long, String>()  // <3>
            .setTypes(Long.class, String.class)   // <4>
            .setStoreByValue(false)   // <5>
            .setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE));  // <6>
    Cache<Long, String> cache = cacheManager.createCache("jCache", configuration); // <7>
    cache.put(1L, "one"); // <8>
    String value = cache.get(1L); // <9>
    // end::basicConfigurationExample[]
    assertThat(value, is("one"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGettingToEhcacheConfiguration() {
    // tag::mutableConfigurationExample[]
    MutableConfiguration<Long, String> configuration = new MutableConfiguration<>();
    configuration.setTypes(Long.class, String.class);
    Cache<Long, String> cache = cacheManager.createCache("someCache", configuration); // <1>

    CompleteConfiguration<Long, String> completeConfiguration = cache.getConfiguration(CompleteConfiguration.class); // <2>

    Eh107Configuration<Long, String> eh107Configuration = cache.getConfiguration(Eh107Configuration.class); // <3>

    CacheRuntimeConfiguration<Long, String> runtimeConfiguration = eh107Configuration.unwrap(CacheRuntimeConfiguration.class); // <4>
    // end::mutableConfigurationExample[]
    assertThat(completeConfiguration, notNullValue());
    assertThat(runtimeConfiguration, notNullValue());

    // Check uses default JSR-107 expiry
    long nanoTime = System.nanoTime();
    LOGGER.info("Seeding random with {}", nanoTime);
    Random random = new Random(nanoTime);
    assertThat(runtimeConfiguration.getExpiryPolicy().getExpiryForCreation(random.nextLong(), Long.toOctalString(random.nextLong())),
                equalTo(org.ehcache.expiry.ExpiryPolicy.INFINITE));
    assertThat(runtimeConfiguration.getExpiryPolicy().getExpiryForAccess(random.nextLong(),
      () -> Long.toOctalString(random.nextLong())), nullValue());
    assertThat(runtimeConfiguration.getExpiryPolicy().getExpiryForUpdate(random.nextLong(),
      () -> Long.toOctalString(random.nextLong()), Long.toOctalString(random.nextLong())), nullValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testUsingEhcacheConfiguration() throws Exception {
    // tag::ehcacheBasedConfigurationExample[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
        ResourcePoolsBuilder.heap(10)).build(); // <1>

    Cache<Long, String> cache = cacheManager.createCache("myCache",
        Eh107Configuration.fromEhcacheCacheConfiguration(cacheConfiguration)); // <2>

    Eh107Configuration<Long, String> configuration = cache.getConfiguration(Eh107Configuration.class);
    configuration.unwrap(CacheConfiguration.class); // <3>

    configuration.unwrap(CacheRuntimeConfiguration.class); // <4>

    try {
      cache.getConfiguration(CompleteConfiguration.class); // <5>
      throw new AssertionError("IllegalArgumentException expected");
    } catch (IllegalArgumentException iaex) {
      // Expected
    }
    // end::ehcacheBasedConfigurationExample[]
  }

  @Test
  public void testWithoutEhcacheExplicitDependencyCanSpecifyXML() throws Exception {
    // tag::jsr107UsingXMLConfigExample[]
    CachingProvider cachingProvider = Caching.getCachingProvider();
    CacheManager manager = cachingProvider.getCacheManager( // <1>
        getClass().getResource("/org/ehcache/docs/ehcache-jsr107-config.xml").toURI(), // <2>
        getClass().getClassLoader()); // <3>
    Cache<Long, Product> readyCache = manager.getCache("ready-cache", Long.class, Product.class); // <4>
    // end::jsr107UsingXMLConfigExample[]
    assertThat(readyCache, notNullValue());
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testWithoutEhcacheExplicitDependencyAndNoCodeChanges() throws Exception {
    CacheManager manager = cachingProvider.getCacheManager(
        getClass().getResource("/org/ehcache/docs/ehcache-jsr107-template-override.xml").toURI(),
        getClass().getClassLoader());

    // tag::jsr107SupplementWithTemplatesExample[]
    MutableConfiguration<Long, Client> mutableConfiguration = new MutableConfiguration<>();
    mutableConfiguration.setTypes(Long.class, Client.class); // <1>

    Cache<Long, Client> anyCache = manager.createCache("anyCache", mutableConfiguration); // <2>

    CacheRuntimeConfiguration<Long, Client> ehcacheConfig = (CacheRuntimeConfiguration<Long, Client>)anyCache.getConfiguration(
        Eh107Configuration.class).unwrap(CacheRuntimeConfiguration.class); // <3>
    ehcacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(); // <4>

    Cache<Long, Client> anotherCache = manager.createCache("byRefCache", mutableConfiguration);
    assertFalse(anotherCache.getConfiguration(Configuration.class).isStoreByValue()); // <5>

    MutableConfiguration<String, Client> otherConfiguration = new MutableConfiguration<>();
    otherConfiguration.setTypes(String.class, Client.class);
    otherConfiguration.setExpiryPolicyFactory(CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE)); // <6>

    Cache<String, Client> foosCache = manager.createCache("foos", otherConfiguration);// <7>
    CacheRuntimeConfiguration<Long, Client> foosEhcacheConfig = (CacheRuntimeConfiguration<Long, Client>)foosCache.getConfiguration(
        Eh107Configuration.class).unwrap(CacheRuntimeConfiguration.class);
    Client client1 = new Client("client1", 1);
    foosEhcacheConfig.getExpiryPolicy().getExpiryForCreation(42L, client1).toMinutes(); // <8>

    CompleteConfiguration<String, String> foosConfig = foosCache.getConfiguration(CompleteConfiguration.class);

    try {
      final Factory<ExpiryPolicy> expiryPolicyFactory = foosConfig.getExpiryPolicyFactory();
      ExpiryPolicy expiryPolicy = expiryPolicyFactory.create(); // <9>
      throw new AssertionError("Expected UnsupportedOperationException");
    } catch (UnsupportedOperationException e) {
      // Expected
    }
    // end::jsr107SupplementWithTemplatesExample[]
    assertThat(ehcacheConfig.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), is(20L));
    assertThat(foosEhcacheConfig.getExpiryPolicy().getExpiryForCreation(42L, client1),
        is(java.time.Duration.ofMinutes(2)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testTemplateOverridingStoreByValue() throws Exception {
    cacheManager = cachingProvider.getCacheManager(
        getClass().getResource("/org/ehcache/docs/ehcache-jsr107-template-override.xml").toURI(),
        getClass().getClassLoader());

    MutableConfiguration<Long, Client> mutableConfiguration = new MutableConfiguration<>();
    mutableConfiguration.setTypes(Long.class, Client.class);

    Client client1 = new Client("client1", 1);

    Cache<Long, Client> myCache = null;
    myCache = cacheManager.createCache("anyCache", mutableConfiguration);
    myCache.put(1L, client1);
    assertNotSame(client1, myCache.get(1L));
    assertTrue(myCache.getConfiguration(Configuration.class).isStoreByValue());

    myCache = cacheManager.createCache("byRefCache", mutableConfiguration);
    myCache.put(1L, client1);
    assertSame(client1, myCache.get(1L));
    assertFalse(myCache.getConfiguration(Configuration.class).isStoreByValue());

    myCache = cacheManager.createCache("weirdCache1", mutableConfiguration);
    myCache.put(1L, client1);
    assertNotSame(client1, myCache.get(1L));
    assertTrue(myCache.getConfiguration(Configuration.class).isStoreByValue());

    myCache = cacheManager.createCache("weirdCache2", mutableConfiguration);
    myCache.put(1L, client1);
    assertSame(client1, myCache.get(1L));
    assertFalse(myCache.getConfiguration(Configuration.class).isStoreByValue());
  }

  @Test
  public void testTemplateOverridingStoreByRef() throws Exception {
    cacheManager = cachingProvider.getCacheManager(
        getClass().getResource("/org/ehcache/docs/ehcache-jsr107-template-override.xml").toURI(),
        getClass().getClassLoader());

    MutableConfiguration<Long, Client> mutableConfiguration = new MutableConfiguration<>();
    mutableConfiguration.setTypes(Long.class, Client.class).setStoreByValue(false);

    Cache<Long, Client> myCache;
    Client client1 = new Client("client1", 1);

    myCache = cacheManager.createCache("anotherCache", mutableConfiguration);
    myCache.put(1L, client1);
    assertSame(client1, myCache.get(1L));

    myCache = cacheManager.createCache("byValCache", mutableConfiguration);
    myCache.put(1L, client1);
    assertNotSame(client1, myCache.get(1L));
  }

  @Test
  public void testCacheThroughAtomicsXMLValid() throws Exception {
    cacheManager = cachingProvider.getCacheManager(
        getClass().getResource("/org/ehcache/docs/ehcache-jsr107-cache-through.xml").toURI(),
        getClass().getClassLoader());
  }

  @Test
  public void testCacheManagerLevelConfiguration() throws Exception {
    // tag::ehcacheCacheManagerConfigurationExample[]
    CachingProvider cachingProvider = Caching.getCachingProvider();
    EhcacheCachingProvider ehcacheProvider = (EhcacheCachingProvider) cachingProvider; // <1>

    DefaultConfiguration configuration = new DefaultConfiguration(ehcacheProvider.getDefaultClassLoader(),
      new DefaultPersistenceConfiguration(getPersistenceDirectory())); // <2>

    CacheManager cacheManager = ehcacheProvider.getCacheManager(ehcacheProvider.getDefaultURI(), configuration); // <3>
    // end::ehcacheCacheManagerConfigurationExample[]

    assertThat(cacheManager, notNullValue());
  }

  private File getPersistenceDirectory() {
    return tempFolder.getRoot();
  }
}
