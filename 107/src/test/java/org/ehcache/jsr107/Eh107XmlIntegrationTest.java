package org.ehcache.jsr107;

import org.junit.Before;
import org.junit.Test;

import com.pany.domain.Customer;
import com.pany.domain.Product;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.cache.Cache;
import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.integration.CacheLoader;
import javax.cache.integration.CacheLoaderException;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

/**
 * StupidTest
 */
public class Eh107XmlIntegrationTest {

  private CacheManager cacheManager;

  @Before
  public void setUp() throws Exception {
    CachingProvider cachingProvider = Caching.getCachingProvider();
    cacheManager = cachingProvider.getCacheManager(getClass().getResource("/ehcache-107-integration.xml")
        .toURI(), cachingProvider.getDefaultClassLoader());
  }

  @Test
  public void test107BasedOnEhcacheTemplate() {
    final DumbCacheLoader product2CacheLoader = new DumbCacheLoader();

    MutableConfiguration<Long, Product> product2Configuration = new MutableConfiguration<Long, Product>();
    product2Configuration.setTypes(Long.class, Product.class).setReadThrough(true);
    product2Configuration.setCacheLoaderFactory(new Factory<CacheLoader<Long, Product>>() {
      @Override
      public CacheLoader<Long, Product> create() {
        return product2CacheLoader;
      }
    });

    Cache<Long, Product> productCache2 = cacheManager.createCache("productCache2", product2Configuration);
    Product product = productCache2.get(124L);
    assertThat(product, notNullValue());
    assertThat(product2CacheLoader.seen, contains(124L));
  }

  @Test
  public void testXmlExampleIn107() throws Exception {

    javax.cache.Cache<Long, Product> productCache = cacheManager.getCache("productCache", Long.class, Product.class);
    assertThat(productCache, is(notNullValue()));
    Configuration<Long, Product> configuration = productCache.getConfiguration(Configuration.class);
    assertThat(configuration.getKeyType(), is(equalTo(Long.class)));
    assertThat(configuration.getValueType(), is(equalTo(Product.class)));

    Eh107ReverseConfiguration eh107ReverseConfiguration = productCache.getConfiguration(Eh107ReverseConfiguration.class);
    assertThat(eh107ReverseConfiguration.isReadThrough(), is(true));
    assertThat(eh107ReverseConfiguration.isWriteThrough(), is(true));
    assertThat(eh107ReverseConfiguration.isStoreByValue(), is(true));

    Product product = new Product(1L);
    productCache.put(1L, product);
    assertThat(productCache.get(1L).getId(), equalTo(product.getId()));

    javax.cache.Cache<Long, Customer> customerCache = cacheManager.getCache("customerCache", Long.class, Customer.class);
    assertThat(customerCache, is(notNullValue()));
    Configuration<Long, Customer> customerConfiguration = customerCache.getConfiguration(Configuration.class);
    assertThat(customerConfiguration.getKeyType(), is(equalTo(Long.class)));
    assertThat(customerConfiguration.getValueType(), is(equalTo(Customer.class)));

    Customer customer = new Customer(1L);
    customerCache.put(1L, customer);
    assertThat(customerCache.get(1L).getId(), equalTo(customer.getId()));
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
      throw new UnsupportedOperationException("TODO Implement me!");
    }
  }
}
