package org.ehcache.jsr107;

import org.junit.Test;

import com.pany.domain.Customer;
import com.pany.domain.Product;

import javax.cache.Caching;
import javax.cache.configuration.Configuration;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * StupidTest
 */
public class Eh107XmlIntegrationTest {
  @Test
  public void testXmlExampleIn107() throws Exception {
    CachingProvider cachingProvider = Caching.getCachingProvider();
    javax.cache.CacheManager cacheManager = cachingProvider.getCacheManager(getClass().getResource("/ehcache-example.xml")
        .toURI(), cachingProvider.getDefaultClassLoader());

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
}
