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

package org.ehcache.clustered.client;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.PersistentCacheManager;
import org.ehcache.Status;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThat;
import org.junit.Before;

/**
 * Tests basic XML configuration of clustered {@link PersistentCacheManager}.
 *
 * @author Clifford W. Johnson
 */
public class SimpleClusteredCacheByXmlTest {

  private static final String SIMPLE_CLUSTER_XML = "/configs/simple-cluster.xml";

  @Before
  public void resetPassthroughServer() {
    UnitTestConnectionService.reset();
  }

  @Test
  public void testViaXml() throws Exception {
    final Configuration configuration = new XmlConfiguration(this.getClass().getResource(SIMPLE_CLUSTER_XML));
    final CacheManager cacheManager = CacheManagerBuilder.newCacheManager(configuration);

    assertThat(cacheManager, is(instanceOf(PersistentCacheManager.class)));

    cacheManager.init();

    final Cache<Long, String> cache = cacheManager.getCache("simple-cache", Long.class, String.class);
    assertThat(cache, is(not(nullValue())));

    if (cacheManager.getStatus() != Status.UNINITIALIZED) {
      cacheManager.close();
    }
  }
}
