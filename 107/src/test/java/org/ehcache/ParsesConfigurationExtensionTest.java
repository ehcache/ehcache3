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

import com.pany.domain.Customer;
import com.pany.domain.Product;
import com.pany.ehcache.integration.ProductCacheLoaderWriter;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.CacheRuntimeConfiguration;
import org.ehcache.core.EhcacheManager;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.expiry.ExpiryPolicy;
import org.ehcache.jsr107.config.Jsr107Configuration;
import org.ehcache.config.ResourceType;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.jsr107.internal.DefaultJsr107Service;
import org.ehcache.spi.service.Service;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Alex Snaps
 */
public class ParsesConfigurationExtensionTest {

  @Test
  public void testConfigParse() throws ClassNotFoundException, SAXException, InstantiationException, IllegalAccessException, IOException {
    final XmlConfiguration configuration = new XmlConfiguration(this.getClass().getResource("/ehcache-107.xml"));
    final DefaultJsr107Service jsr107Service = new DefaultJsr107Service(ServiceUtils.findSingletonAmongst(Jsr107Configuration.class, configuration.getServiceCreationConfigurations().toArray()));

    final CacheManager cacheManager = new EhcacheManager(configuration, Collections.<Service>singletonList(jsr107Service));
    cacheManager.init();

    assertThat(jsr107Service.getTemplateNameForCache("foos"), equalTo("stringCache"));
    assertThat(jsr107Service.getTemplateNameForCache("bars"), equalTo("tinyCache"));
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testXmlExample() throws ClassNotFoundException, SAXException, InstantiationException, IOException, IllegalAccessException {
    XmlConfiguration config = new XmlConfiguration(ParsesConfigurationExtensionTest.class.getResource("/ehcache-example.xml"));
    final DefaultJsr107Service jsr107Service = new DefaultJsr107Service(ServiceUtils.findSingletonAmongst(Jsr107Configuration.class, config.getServiceCreationConfigurations().toArray()));

    final CacheManager cacheManager = new EhcacheManager(config, Collections.<Service>singletonList(jsr107Service));
    cacheManager.init();

    // test productCache
    {
      final Cache<Long, Product> productCache = cacheManager.getCache("productCache", Long.class, Product.class);
      assertThat(productCache, notNullValue());

      // Test the config
      {
        final CacheRuntimeConfiguration<Long, Product> runtimeConfiguration = productCache.getRuntimeConfiguration();
        assertThat(runtimeConfiguration.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(200L));

        final ExpiryPolicy<? super Long, ? super Product> expiry = runtimeConfiguration.getExpiryPolicy();
        assertThat(expiry.getClass().getName(), equalTo("org.ehcache.config.builders.ExpiryPolicyBuilder$TimeToIdleExpiryPolicy"));
        assertThat(expiry.getExpiryForAccess(42L, null), equalTo(Duration.ofMinutes(2)));

        assertThat(runtimeConfiguration.getEvictionAdvisor(), instanceOf(com.pany.ehcache.MyEvictionAdvisor.class));
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
        final List<Product> products = ProductCacheLoaderWriter.written.get(value.getId());
        assertThat(products, notNullValue());
        assertThat(products.get(0), sameInstance(value));
      }
    }

    // Test template
    {
      final CacheConfigurationBuilder<Object, Object> myDefaultTemplate = config.newCacheConfigurationBuilderFromTemplate("myDefaultTemplate", Object.class, Object.class, heap(10));
      assertThat(myDefaultTemplate, notNullValue());
    }

    // Test customerCache templated cache
    {
      final Cache<Long, Customer> customerCache = cacheManager.getCache("customerCache", Long.class, Customer.class);
      final CacheRuntimeConfiguration<Long, Customer> runtimeConfiguration = customerCache.getRuntimeConfiguration();
      assertThat(runtimeConfiguration.getResourcePools().getPoolForResource(ResourceType.Core.HEAP).getSize(), equalTo(200L));
    }
  }
}
