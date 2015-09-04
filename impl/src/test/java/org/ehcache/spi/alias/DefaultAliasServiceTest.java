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

package org.ehcache.spi.alias;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.spi.ServiceProvider;
import org.ehcache.spi.service.Service;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.ehcache.spi.alias.DefaultAliasConfiguration.cacheManagerAlias;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Mathieu Carbou
 */
public class DefaultAliasServiceTest {

  AliasService unNamedIdServ;
  AliasService namedIdServ;
  CacheManager unnamedCM;
  CacheManager namedCM;

  @Before
  public void before() {
    CacheConfiguration<String, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(String.class, String.class);

    unnamedCM = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache3", cacheConfiguration)
        .using(unNamedIdServ = new DefaultAliasService())
        .build(true);

    namedCM = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .using(cacheManagerAlias("my-cm"))
        .using(new Service() {
          @Override
          public void start(ServiceProvider serviceProvider) {
            namedIdServ = serviceProvider.getService(AliasService.class);
          }

          @Override
          public void stop() {

          }
        })
        .build(true);
  }

  @After
  public void after() {
    unnamedCM.close();
    namedCM.close();
  }

  @Test
  public void test_identification_service() throws Exception {
    assertTrue(unNamedIdServ.getCacheManagerAlias().matches("cache-manager-\\d+"));
    assertEquals("my-cm", namedIdServ.getCacheManagerAlias());
    assertEquals("aCache1", namedIdServ.getCacheAlias(namedCM.getCache("aCache1", String.class, String.class)));

    Cache<String, String> cache3 = unnamedCM.getCache("aCache3", String.class, String.class);

    assertEquals("aCache3", unNamedIdServ.getCacheAlias(cache3));

    try {
      namedIdServ.getCacheAlias(cache3);
      fail();
    } catch (Exception e) {
      assertEquals(IllegalArgumentException.class, e.getClass());
    }
  }

}
