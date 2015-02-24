package org.ehcache.jsr107;

import com.pany.domain.Product;
import com.pany.ehcache.integration.ProductCacheLoaderWriter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.cache.Caching;
import javax.cache.configuration.Factory;
import javax.cache.configuration.MutableConfiguration;
import javax.cache.expiry.CreatedExpiryPolicy;
import javax.cache.expiry.Duration;
import javax.cache.expiry.EternalExpiryPolicy;
import javax.cache.expiry.ExpiryPolicy;
import javax.cache.integration.CacheLoader;
import javax.cache.spi.CachingProvider;

import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

public class Eh107JSRTemplateOverridingTest {

  private javax.cache.CacheManager cm;

  @Before
  public void setup() throws Exception {
    CachingProvider provider = Caching.getCachingProvider();
    cm = provider.getCacheManager(this.getClass().getResource("/ehcache-107-overriding.xml").toURI(),
        getClass().getClassLoader());
  }

  @After
  public void cleanUp() throws Exception {
    if (cm != null) {
      cm.close();
    }
  }

  private MutableConfiguration createMutableConfiguration(Class k, Class v) {
    MutableConfiguration mConf = new MutableConfiguration();
    mConf.setTypes(k, v);
    return mConf;
  }

  private MutableConfiguration<Long, String> createDefaultCacheConf() {
    return createMutableConfiguration(Long.class, String.class);
  }

  private MutableConfiguration<Long, Product> createProductCacheConf() {
    return createMutableConfiguration(Long.class, Product.class);
  }

  @Test
  public void testExpiryConfigurationPolicy() {
    MutableConfiguration<Long, String> conf =  createDefaultCacheConf();
    javax.cache.Cache<Long, String> c1 = cm.createCache("c1", conf);
    Eh107CompleteConfiguration<Long, String> cc1 = c1.getConfiguration(Eh107CompleteConfiguration.class);
    assertThat(cc1.getExpiryPolicyFactory(), is(EternalExpiryPolicy.factoryOf()));

    MutableConfiguration<Long, String> c2Conf = createDefaultCacheConf();
    Factory<ExpiryPolicy> factory = CreatedExpiryPolicy.factoryOf(Duration.ONE_MINUTE);
    c2Conf.setExpiryPolicyFactory(factory);
    javax.cache.Cache<Long, String> c2 = cm.createCache("c2", c2Conf);
    Eh107CompleteConfiguration<Long, String> cc2 = c2.getConfiguration(Eh107CompleteConfiguration.class);
    assertThat(cc2.getExpiryPolicyFactory(), is(factory));

    Eh107MutableConfiguration<Long, String> c3Conf = new Eh107MutableConfiguration<Long, String>();
    c3Conf.setTypes(Long.class, String.class);
    javax.cache.Cache<Long, String> c3 = cm.createCache("c3", c3Conf);
    c3.put(1L, "a");
    try {
      Thread.sleep(10);
    } catch (InterruptedException e) {
      //ignore it
    }
    assertThat(c3.get(1L), is(nullValue()));
  }


  @Test
  public void testCacheLoaderWriterOverriding() {
    javax.cache.Cache<Long, Product> c1 = cm.createCache("productCache", createProductCacheConf());
    ProductCacheLoaderWriter.written.clear();
    c1.put(1L, new Product(1L));
    assertThat(ProductCacheLoaderWriter.written.isEmpty(), is(true));
    cm.destroyCache("productCache");
    MutableConfiguration conf = createProductCacheConf();
    conf.setReadThrough(true);
    c1 = cm.createCache("productCache", conf);
    c1.put(1L, new Product(1L));
    assertThat(ProductCacheLoaderWriter.written.isEmpty(), is(false));
    cm.destroyCache("productCache");

    conf = createProductCacheConf();
    ProductCacheLoaderWriter.written.clear();
    conf.setReadThrough(false);
    conf.setWriteThrough(true);
    c1 = cm.createCache("productCache", conf);
    c1.put(1L, new Product(1L));
    assertThat(ProductCacheLoaderWriter.written.isEmpty(), is(false));
    ProductCacheLoaderWriter.written.clear();
    cm.destroyCache("productCache");
    try {
      conf = createProductCacheConf();
      conf.setReadThrough(true);
      cm.createCache("p1", conf);
      fail("Cache creation should have failed for illegal cache loader/writer configuration");
    } catch (MultiCacheException e) {
      assertThat(e.getThrowables().get(0).getMessage(),
          is(equalTo("Unable to construct (read/write)through cache without either a templated loader/writer or configured loader/writer")));
    }
  }


  @Test
  public void testLoaderWriterFactoryOverridingInConf() {
    MutableConfiguration<Long, Product> conf = createProductCacheConf();
    conf.setReadThrough(true);
    final Eh107XmlIntegrationTest.DumbCacheLoader loader = new Eh107XmlIntegrationTest.DumbCacheLoader();
    conf.setCacheLoaderFactory(new Factory<CacheLoader<Long, Product>>() {
      @Override
      public CacheLoader<Long, Product> create() {
        return loader;
      }
    });
    javax.cache.Cache<Long, Product> c1 = cm.createCache("productCache", conf);
    c1.get(1L);
    assertThat(loader.seen.contains(1L), is(true));
  }


}
