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

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.Configuration;
import org.junit.Test;
import org.xml.sax.SAXException;

import com.pany.ehcache.integration.TestCacheLoaderWriter;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/**
 * @author Alex Snaps
 */
public class IntegrationConfigTest {

  @Test
  public void testLoaderWriter() throws ClassNotFoundException, SAXException, InstantiationException,
      IOException, IllegalAccessException {
    Configuration configuration = new XmlConfiguration(this.getClass().getResource("/configs/cache-integration.xml"));
    assertThat(configuration.getCacheConfigurations().containsKey("bar"), is(true));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);
    final Cache<Number, String> cache = cacheManager.getCache("bar", Number.class, String.class);
    assertThat(cache, notNullValue());
    assertThat(cache.get(1), notNullValue());
    final Number key = new Long(42);
    cache.put(key, "Bye y'all!");
    assertThat(TestCacheLoaderWriter.lastWrittenKey, is(key));
  }
}
