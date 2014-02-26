/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.config.xml;

import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.Configuration;
import org.ehcache.spi.ServiceConfiguration;
import org.xml.sax.SAXParseException;

import static org.hamcrest.collection.IsCollectionWithSize.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsCollectionContaining.*;
import static org.hamcrest.core.IsInstanceOf.*;
import static org.hamcrest.core.IsNot.*;
import static org.hamcrest.core.IsNull.*;
import static org.hamcrest.core.IsSame.*;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 *
 * @author Chris Dennis
 */
public class XmlConfigurationTest {
  
  @Test
  public void testOneServiceConfig() throws Exception {
    XmlConfiguration xmlConfig = new XmlConfiguration();
    Configuration config = xmlConfig.parseConfiguration(XmlConfigurationTest.class.getResource("/configs/one-service.xml"));
    
    assertThat(config.getServiceConfigurations(), IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(FooConfiguration.class)));
    assertThat(config.getCacheConfigurations().keySet(), hasSize(0));
  }

  @Test
  public void testOneCacheConfig() throws Exception {
    XmlConfiguration xmlConfig = new XmlConfiguration();
    Configuration config = xmlConfig.parseConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"));
    
    assertThat(config.getServiceConfigurations(), hasSize(0));
    assertThat(config.getCacheConfigurations().keySet(), hasItem("bar"));
    assertThat(config.getCacheConfigurations().get("bar").getServiceConfigurations(), IsCollectionContaining.<ServiceConfiguration<?>>hasItem(instanceOf(FooConfiguration.class)));
    
    CacheManager manager = new CacheManager(config);

    Cache<String, String> cache = manager.getCache("bar", String.class, String.class);
    assertThat(cache, not(nullValue()));
    assertThat(cache.getAndPut("bar", "bat"), nullValue());
    assertThat(cache.get("bar"), is("bat"));
  }

  @Test
  public void testInvalidCoreConfiguration() throws Exception {
    XmlConfiguration xmlConfig = new XmlConfiguration();
    try {
      xmlConfig.parseConfiguration(XmlConfigurationTest.class.getResource("/configs/invalid-core.xml"));
      fail();
    } catch (SAXParseException e) {
      assertThat(e.getLineNumber(), is(5));
      assertThat(e.getColumnNumber(), is(74));
    }
  }
  
  @Test
  public void testInvalidServiceConfiguration() throws Exception {
    XmlConfiguration xmlConfig = new XmlConfiguration();
    try {
      xmlConfig.parseConfiguration(XmlConfigurationTest.class.getResource("/configs/invalid-service.xml"));
    } catch (SAXParseException e) {
      assertThat(e.getLineNumber(), is(6));
      assertThat(e.getColumnNumber(), is(15));
    }
  }
  
  @Test
  public void testParserReuse() throws Exception {
    XmlConfiguration xmlConfig = new XmlConfiguration();

    Configuration configOne = xmlConfig.parseConfiguration(XmlConfigurationTest.class.getResource("/configs/one-cache.xml"));
    Configuration configTwo = xmlConfig.parseConfiguration(XmlConfigurationTest.class.getResource("/configs/one-service.xml"));
    
    assertThat(configTwo, not(sameInstance(configOne)));
  }
}
