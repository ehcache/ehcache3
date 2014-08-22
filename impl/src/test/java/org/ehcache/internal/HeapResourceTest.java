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

package org.ehcache.internal;

import org.ehcache.Cache;
import org.ehcache.DefaultCacheManager;
import org.ehcache.spi.ServiceProvider;
import org.junit.Test;
import org.ehcache.internal.cachingtier.TieredCache;
import org.ehcache.config.Configuration;
import org.ehcache.config.xml.XmlConfiguration;
import org.xml.sax.SAXException;

import java.io.IOException;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * @author Alex Snaps
 */
public class HeapResourceTest {

  @Test
  public void testGetsConfig() throws IOException, SAXException, ClassNotFoundException {
    XmlConfiguration xmlConfiguration = new XmlConfiguration();
    final Configuration configuration =
        xmlConfiguration.parseConfiguration(HeapResourceTest.class.getResource("/configs/cachingtier-cache.xml"));
    DefaultCacheManager defaultCacheManager = new DefaultCacheManager(configuration);
    try {
      final Cache<String, String> foo = defaultCacheManager.getCache("foo", String.class, String.class);
      assertThat((Class) foo.getClass(), sameInstance((Class) TieredCache.class));
      assertThat(((TieredCache<String, String>)foo).getMaxCacheSize(), is(4321L));
    } finally {
      defaultCacheManager.close();
    }
  }
}
