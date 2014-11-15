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
import org.ehcache.config.Configuration;
import org.ehcache.config.xml.XmlConfiguration;
import org.ehcache.jsr107.DefaultJsr107Service;
import org.ehcache.spi.ServiceLocator;
import org.junit.Test;
import org.xml.sax.SAXException;

import java.io.IOException;

import javax.cache.configuration.CompleteConfiguration;
import javax.cache.configuration.MutableConfiguration;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Alex Snaps
 */
public class ParsesConfigExtensionTest {

  @Test
  public void testConfigParse() throws ClassNotFoundException, SAXException, InstantiationException, IllegalAccessException, IOException {
    XmlConfiguration xmlConfiguration = new XmlConfiguration(this.getClass().getResource("/ehcache-107.xml"));
    final Configuration configuration = xmlConfiguration.parseConfiguration();
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

    CacheConfigurationBuilder configurationBuilder = xmlConfiguration
        .newCacheConfigurationBuilderFromTemplate(jsr107Service.getDefaultTemplate(), cfg.getKeyType(), cfg.getValueType());
    final String template = jsr107Service.getTemplateNameForCache(name);
    if(template != null) {
      configurationBuilder = xmlConfiguration
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
}
