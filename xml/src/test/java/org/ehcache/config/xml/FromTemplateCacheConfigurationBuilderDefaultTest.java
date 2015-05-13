package org.ehcache.config.xml;

import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.internal.store.heap.service.OnHeapStoreServiceConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;

/**
 * TemplateDefaultTest
 */
public class FromTemplateCacheConfigurationBuilderDefaultTest {

  private XmlConfiguration xmlConfiguration;
  private CacheConfigurationBuilder<Object, Object> minimalTemplateBuilder;

  @Before
  public void setUp() throws Exception {
    xmlConfiguration = new XmlConfiguration(getClass().getResource("/configs/template-defaults.xml"));
    minimalTemplateBuilder = xmlConfiguration.newCacheConfigurationBuilderFromTemplate("minimal-template");
  }

  @Test
  public void testDefaultStoreByRefCreatesNoOnHeapStoreServiceConfig() throws Exception {
    assertThat(minimalTemplateBuilder.getExistingServiceConfiguration(OnHeapStoreServiceConfiguration.class), nullValue());
  }

  @Test
  public void testDefaultExpiry() throws Exception {
    assertThat(minimalTemplateBuilder.hasDefaultExpiry(), is(true));
  }
}
