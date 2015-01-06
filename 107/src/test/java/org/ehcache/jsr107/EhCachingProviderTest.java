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

package org.ehcache.jsr107;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.Properties;

import javax.cache.CacheManager;
import javax.cache.Caching;
import javax.cache.spi.CachingProvider;

import org.junit.Test;

public class EhCachingProviderTest {

  @Test
  public void testLoadsAsCachingProvider() {
    final CachingProvider provider = Caching.getCachingProvider();
    assertThat(provider, is(instanceOf(EhcacheCachingProvider.class)));
  }

  @Test
  public void testDefaultUriOverride() throws Exception {
    URI override = getClass().getResource("/ehcache-107.xml").toURI();

    Properties props = new Properties();
    props.put(DefaultConfigResolver.DEFAULT_CONFIG_PROPERTY_NAME, override);

    CacheManager cacheManager = Caching.getCachingProvider().getCacheManager(null, null, props);

    assertEquals(override, cacheManager.getURI());

    Caching.getCachingProvider().close();
  }

}
