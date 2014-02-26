/*
 * All content copyright Terracotta, Inc., unless otherwise indicated. All rights reserved.
 */

package org.ehcache.internal;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
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
  public void testGetsConfig() throws IOException, SAXException, ClassNotFoundException, InterruptedException {
    XmlConfiguration xmlConfiguration = new XmlConfiguration();
    final Configuration configuration =
        xmlConfiguration.parseConfiguration(HeapResourceTest.class.getResource("/configs/cachingtier-cache.xml"));
    CacheManager cacheManager = new CacheManager(configuration);
    try {
      final Cache<String, String> foo = cacheManager.getCache("foo", String.class, String.class);
      assertThat((Class) foo.getClass(), sameInstance((Class) TieredCache.class));
      assertThat(((TieredCache<String, String>)foo).getMaxCacheSize(), is(4321L));
    } finally {
      cacheManager.stop();
    }
  }
}
