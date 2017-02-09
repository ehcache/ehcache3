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

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;

import com.pany.domain.Client;
import com.pany.domain.Customer;
import com.pany.domain.Product;
import com.pany.ehcache.Test107CacheEntryListener;
import com.pany.ehcache.TestCacheEventListener;
import com.pany.ehcache.integration.ProductCacheLoaderWriter;

import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableCacheEntryListenerConfiguration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.event.CacheEntryListener;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.integration.CompletionListenerFuture;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;

public class Eh107XmlIntegrationTest {

  private CacheManager cacheManager;
  private CachingProvider cachingProvider;

  @Before
  public void setUp() throws Exception {
    cachingProvider = Caching.getCachingProvider();
    cacheManager = cachingProvider.getCacheManager(getClass().getResource("/ehcache-107-integration.xml")
        .toURI(), cachingProvider.getDefaultClassLoader());
  }

  @Test
  public void test107CacheCanReturnCompleteConfigurationWhenNonePassedIn() {
    CacheManager cacheManager = cachingProvider.getCacheManager();
    Cache<Long, String> cache = cacheManager.createCache("cacheWithoutCompleteConfig", new Configuration<Long, String>() {
      @Override
      public Class<Long> getKeyType() {
        return Long.class;
      }

      @Override
      public Class<String> getValueType() {
        return String.class;
      }

      @Override
      public boolean isStoreByValue() {
        return true;
      }
    });

    @SuppressWarnings("unchecked")
    CompleteConfiguration<Long, String> configuration = cache.getConfiguration(CompleteConfiguration.class);
    assertThat(configuration, notNullValue());
    assertThat(configuration.isStoreByValue(), is(true));

    // Respects defaults
    assertThat(configuration.getExpiryPolicyFactory(), equalTo(EternalExpiryPolicy.factoryOf()));
  }

  @Test
  public void testTemplateAddsListeners() throws Exception {
    CacheManager cacheManager = cachingProvider.getCacheManager(getClass().getResource("/ehcache-107-listeners.xml")
        .toURI(), getClass().getClassLoader());

    MutableConfiguration<String, String> configuration = new MutableConfiguration<String, String>();
    configuration.setTypes(String.class, String.class);
    MutableCacheEntryListenerConfiguration<String, String> listenerConfiguration = new MutableCacheEntryListenerConfiguration<String, String>(new Factory<CacheEntryListener<? super String, ? super String>>() {
      @Override
      public CacheEntryListener<? super String, ? super String> create() {
        return new Test107CacheEntryListener();
      }
    }, null, false, true);
    configuration.addCacheEntryListenerConfiguration(listenerConfiguration);

    Cache<String, String> cache = cacheManager.createCache("foos", configuration);
    cache.put("Hello", "Bonjour");

    assertThat(Test107CacheEntryListener.seen.size(), Matchers.is(1));
    assertThat(TestCacheEventListener.seen.size(), Matchers.is(1));
  }

  @Test
  public void test107LoaderOverriddenByEhcacheTemplateLoaderWriter() throws Exception {
    final AtomicBoolean loaderFactoryInvoked = new AtomicBoolean(false);
    final DumbCacheLoader product2CacheLoader = new DumbCacheLoader();

    MutableConfiguration<Long, Product> product2Configuration = new MutableConfiguration<Long, Product>();
    product2Configuration.setTypes(Long.class, Product.class).setReadThrough(true);
    product2Configuration.setCacheLoaderFactory(new Factory<CacheLoader<Long, Product>>() {
      @Override
      public CacheLoader<Long, Product> create() {
        loaderFactoryInvoked.set(true);
        return product2CacheLoader;
      }
    });

    Cache<Long, Product> productCache2 = cacheManager.createCache("productCache2", product2Configuration);

    assertThat(loaderFactoryInvoked.get(), is(false));

    Product product = productCache2.get(124L);
    assertThat(product.getId(), is(124L));
    assertThat(ProductCacheLoaderWriter.seen, hasItem(124L));
    assertThat(product2CacheLoader.seen, is(empty()));

    CompletionListenerFuture future = new CompletionListenerFuture();
    productCache2.loadAll(Collections.singleton(42L), false, future);
    future.get();
    assertThat(ProductCacheLoaderWriter.seen, hasItem(42L));
    assertThat(product2CacheLoader.seen, is(empty()));
  }

  @Test
  public void test107ExpiryOverriddenByEhcacheTemplateExpiry() {
    final AtomicBoolean expiryFactoryInvoked = new AtomicBoolean(false);
    MutableConfiguration<Long, Product> configuration = new MutableConfiguration<Long, Product>();
    configuration.setTypes(Long.class, Product.class);
    configuration.setExpiryPolicyFactory(new Factory<ExpiryPolicy>() {
      @Override
      public ExpiryPolicy create() {
        expiryFactoryInvoked.set(true);
        return new CreatedExpiryPolicy(Duration.FIVE_MINUTES);
      }
    });

    cacheManager.createCache("productCache3", configuration);
    assertThat(expiryFactoryInvoked.get(), is(false));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void testXmlExampleIn107() throws Exception {
    javax.cache.Cache<Long, Product> productCache = cacheManager.getCache("productCache", Long.class, Product.class);
    assertThat(productCache, is(notNullValue()));
    Configuration<Long, Product> configuration = productCache.getConfiguration(Configuration.class);
    assertThat(configuration.getKeyType(), is(equalTo(Long.class)));
    assertThat(configuration.getValueType(), is(equalTo(Product.class)));

    Eh107ReverseConfiguration<Long, Product> eh107ReverseConfiguration = productCache.getConfiguration(Eh107ReverseConfiguration.class);
    assertThat(eh107ReverseConfiguration.isReadThrough(), is(true));
    assertThat(eh107ReverseConfiguration.isWriteThrough(), is(true));
    assertThat(eh107ReverseConfiguration.isStoreByValue(), is(true));

    Product product = new Product(1L);
    productCache.put(1L, product);
    assertThat(productCache.get(1L).getId(), equalTo(product.getId()));

    product.setMutable("foo");
    assertThat(productCache.get(1L).getMutable(), nullValue());
    assertThat(productCache.get(1L), not(sameInstance(product)));

    javax.cache.Cache<Long, Customer> customerCache = cacheManager.getCache("customerCache", Long.class, Customer.class);
    assertThat(customerCache, is(notNullValue()));
    Configuration<Long, Customer> customerConfiguration = customerCache.getConfiguration(Configuration.class);
    assertThat(customerConfiguration.getKeyType(), is(equalTo(Long.class)));
    assertThat(customerConfiguration.getValueType(), is(equalTo(Customer.class)));

    Customer customer = new Customer(1L);
    customerCache.put(1L, customer);
    assertThat(customerCache.get(1L).getId(), equalTo(customer.getId()));
  }

  @Test
  public void testCopierAtServiceLevel() throws Exception {
    CacheManager cacheManager = cachingProvider.getCacheManager(
        getClass().getResource("/ehcache-107-default-copiers.xml").toURI(),
        getClass().getClassLoader());

    MutableConfiguration<Long, Client> config = new MutableConfiguration<Long, Client>();
    config.setTypes(Long.class, Client.class);
    Cache<Long, Client> bar = cacheManager.createCache("bar", config);
    Client client = new Client("tc", 1000000L);
    long key = 42L;
    bar.put(key, client);
    assertThat(bar.get(key), not(sameInstance(client)));
  }

  static class DumbCacheLoader implements CacheLoader<Long, Product> {

    Set<Long> seen = new HashSet<Long>();

    @Override
    public Product load(Long aLong) throws CacheLoaderException {
      seen.add(aLong);
      return new Product(aLong);
    }

    @Override
    public Map<Long, Product> loadAll(Iterable<? extends Long> iterable) throws CacheLoaderException {
      for (Long aLong : iterable) {
        seen.add(aLong);
      }
      return Collections.emptyMap();
    }
  }
}
