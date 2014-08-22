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

import org.ehcache.config.Configuration;
import org.ehcache.spi.service.ServiceConfiguration;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import org.xml.sax.SAXParseException;

import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;
import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.hamcrest.core.IsNot.not;
import static org.hamcrest.core.IsSame.sameInstance;
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
