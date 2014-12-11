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

package org.ehcache;

import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.config.Eviction;
import org.ehcache.config.EvictionPrioritizer;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expiry;
import org.ehcache.internal.serialization.JavaSerializationProvider;
import org.ehcache.jsr107.DefaultJsr107Service;
import org.ehcache.spi.ServiceLocator;
import org.hamcrest.CoreMatchers;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.pany.domain.Customer;
import com.pany.domain.Product;
import com.pany.ehcache.integration.ProductCacheWriter;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

import javax.cache.Caching;
import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.Configuration;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.spi.CachingProvider;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Alex Snaps
 */
public class ParsesConfigExtensionTest {

  @Test
  public void testConfigParse() throws ClassNotFoundException, SAXException, InstantiationException, IllegalAccessException, IOException {
    final XmlConfiguration configuration = new XmlConfiguration(this.getClass().getResource("/ehcache-107.xml"));
    final DefaultJsr107Service jsr107Service = new DefaultJsr107Service();
    final ServiceLocator serviceLocator = new ServiceLocator(jsr107Service);
    final CacheManager cacheManager = new EhcacheManager(configuration, serviceLocator);
    cacheManager.init();

    assertThat(jsr107Service.getDefaultTemplate(), equalTo("tinyCache"));
    assertThat(jsr107Service.getTemplateNameForCache("foos"), equalTo("stringCache"));
    assertThat(jsr107Service.getTemplateNameForCache("bars"), nullValue());

    // TEST ENDS HERE, BELOW IS AN EXAMPLE OF CODE FOR OUR 107 CACHE MANAGER
    // IT WOULD BUILD OUR 107 CACHE MANAGER INSTANCE AS ABOVE, KEEPING A REF TO THE DefaultJsr107Service ABOVE

    // random javax.cache.CacheManager.createCache(name, cfg) example:
    String name = "bar";
    CompleteConfiguration cfg = new MutableConfiguration();

    CacheConfigurationBuilder configurationBuilder = configuration
        .newCacheConfigurationBuilderFromTemplate(jsr107Service.getDefaultTemplate(), cfg.getKeyType(), cfg.getValueType());
    final String template = jsr107Service.getTemplateNameForCache(name);
    if(template != null) {
      configurationBuilder = configuration
          .newCacheConfigurationBuilderFromTemplate(name, cfg.getKeyType(), cfg.getValueType());
    }
    if(configurationBuilder != null) {
      configurationBuilder = CacheConfigurationBuilder.newCacheConfigurationBuilder();
    }
    // set all attributes from cfg, to complete or override
    // and finally build our CacheConfig:
    final CacheConfiguration cacheConfiguration = configurationBuilder.buildConfig(cfg.getKeyType(), cfg.getValueType());
    // final Cache cache = cacheManager.createCache(name, cacheConfiguration); // throws NPE because of no Store.Provider Service present
  }

  @Test
  public void testXmlExample() throws ClassNotFoundException, SAXException, InstantiationException, IOException, IllegalAccessException {
    XmlConfiguration config = new XmlConfiguration(ParsesConfigExtensionTest.class.getResource("/ehcache-example.xml"));
    final DefaultJsr107Service jsr107Service = new DefaultJsr107Service();
    final ServiceLocator serviceLocator = new ServiceLocator(jsr107Service);
    final CacheManager cacheManager = new EhcacheManager(config, serviceLocator);
    cacheManager.init();

    // test productCache
    {
      final Cache<Long, Product> productCache = cacheManager.getCache("productCache", Long.class, Product.class);
      assertThat(productCache, notNullValue());

      // Test the config
      {
        final CacheRuntimeConfiguration<Long, Product> runtimeConfiguration = productCache.getRuntimeConfiguration();
        assertThat(runtimeConfiguration.getSerializationProvider(), instanceOf(JavaSerializationProvider.class));
        assertThat(runtimeConfiguration.getCapacityConstraint(), CoreMatchers.<Comparable<Long>>is(200L));

        final Expiry<? super Long, ? super Product> expiry = runtimeConfiguration.getExpiry();
        assertThat(expiry.getClass().getName(), equalTo("org.ehcache.expiry.Expirations$TimeToIdleExpiry"));
        assertThat(expiry.getExpiryForAccess(42L, null), equalTo(new Duration(2, TimeUnit.MINUTES)));

        assertThat(runtimeConfiguration.getEvictionVeto(), instanceOf(com.pany.ehcache.MyEvictionVeto.class));
        assertThat(runtimeConfiguration.getEvictionPrioritizer(), is((EvictionPrioritizer) Eviction.Prioritizer.LFU));
      }

      // test copies
      {
        final Product value = new Product(1L);
        productCache.put(value.getId(), value);
        value.setMutable("fool!");
        assertThat(productCache.get(value.getId()).getMutable(), nullValue());
        assertThat(productCache.get(value.getId()), not(sameInstance(productCache.get(value.getId()))));
      }

      // test loader
      {
        final long key = 123L;
        final Product product = productCache.get(key);
        assertThat(product, notNullValue());
        assertThat(product.getId(), equalTo(key));
      }

      // test writer
      {
        final Product value = new Product(42L);
        productCache.put(42L, value);
        final List<Product> products = ProductCacheWriter.written.get(value.getId());
        assertThat(products, notNullValue());
        assertThat(products.get(0), sameInstance(value));
      }
    }

    // Test template
    {
      final CacheConfigurationBuilder<Object, Object> myDefaultTemplate = config.newCacheConfigurationBuilderFromTemplate("myDefaultTemplate");
      assertThat(myDefaultTemplate, notNullValue());
    }

    // Test customerCache templated cache
    {
      final Cache<Long, Customer> customerCache = cacheManager.getCache("customerCache", Long.class, Customer.class);
      final CacheRuntimeConfiguration<Long, Customer> runtimeConfiguration = customerCache.getRuntimeConfiguration();
      assertThat(runtimeConfiguration.getEvictionPrioritizer(), is((EvictionPrioritizer) Eviction.Prioritizer.LRU));
      assertThat(runtimeConfiguration.getCapacityConstraint(), CoreMatchers.<Comparable<Long>>is(200L));
    }
  }
}
