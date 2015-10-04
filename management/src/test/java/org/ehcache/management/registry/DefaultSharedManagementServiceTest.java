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
package org.ehcache.management.registry;

import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.management.SharedManagementService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.stats.primitive.Counter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.ehcache.spi.alias.DefaultAliasConfiguration.cacheManagerAlias;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/**
 * @author Mathieu Carbou
 */
@RunWith(JUnit4.class)
public class DefaultSharedManagementServiceTest {

  CacheManager cacheManager1;
  CacheManager cacheManager2;
  SharedManagementService service;

  @Before
  public void init() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    service = new DefaultSharedManagementService();

    cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .using(service)
        .using(cacheManagerAlias("myCM1"))
        .build(true);

    cacheManager2 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache2", cacheConfiguration)
        .withCache("aCache3", cacheConfiguration)
        .using(service)
        .using(cacheManagerAlias("myCM2"))
        .build(true);
  }

  @After()
  public void close() {
    cacheManager2.close();
    cacheManager1.close();
  }

  @Test
  public void testSharedContexts() {
    List<ContextContainer> ctx = new ArrayList<ContextContainer>(service.contexts());
    assertEquals(2, ctx.size());

    assertThat(ctx.get(0).getName(), equalTo("cacheManagerName"));
    assertThat(ctx.get(0).getValue(), equalTo("myCM1"));

    assertThat(ctx.get(1).getName(), equalTo("cacheManagerName"));
    assertThat(ctx.get(1).getValue(), equalTo("myCM2"));

    assertThat(ctx.get(0).getSubContexts().size(), equalTo(1));
    assertThat(ctx.get(0).getSubContexts().iterator().next().getName(), equalTo("cacheName"));
    assertThat(ctx.get(0).getSubContexts().iterator().next().getValue(), equalTo("aCache1"));

    assertThat(ctx.get(1).getSubContexts().size(), equalTo(2));
    assertThat(ctx.get(1).getSubContexts().iterator().next().getName(), equalTo("cacheName"));
    assertThat(new ArrayList<ContextContainer>(ctx.get(1).getSubContexts()).get(1).getName(), equalTo("cacheName"));

    assertThat(new ArrayList<ContextContainer>(ctx.get(1).getSubContexts()).get(0).getValue(), isIn(Arrays.asList("aCache2", "aCache3")));
    assertThat(new ArrayList<ContextContainer>(ctx.get(1).getSubContexts()).get(1).getValue(), isIn(Arrays.asList("aCache2", "aCache3")));


  }

  @Test
  public void testSharedCapabilities() {
    List<Map<String, String>> contextList = Arrays.<Map<String, String>>asList(
        new HashMap<String, String>() {{
          put("cacheManagerName", "myCM1");
        }},
        new HashMap<String, String>() {{
          put("cacheManagerName", "myCM2");
        }}
    );

    List<Collection<Capability>> allCapabilities = service.capabilities(contextList);

    assertThat(allCapabilities, hasSize(2));

    assertThat(allCapabilities.get(0), hasSize(2));
    assertThat(new ArrayList<Capability>(allCapabilities.get(0)).get(0).getName(), equalTo("org.ehcache.management.providers.actions.EhcacheActionProvider"));
    assertThat(new ArrayList<Capability>(allCapabilities.get(0)).get(1).getName(), equalTo("org.ehcache.management.providers.statistics.EhcacheStatisticsProvider"));

    assertThat(allCapabilities.get(1), hasSize(2));
    assertThat(new ArrayList<Capability>(allCapabilities.get(1)).get(0).getName(), equalTo("org.ehcache.management.providers.actions.EhcacheActionProvider"));
    assertThat(new ArrayList<Capability>(allCapabilities.get(1)).get(1).getName(), equalTo("org.ehcache.management.providers.statistics.EhcacheStatisticsProvider"));
  }

  @Test
  public void testStats() {
    List<Map<String, String>> contextList = Arrays.<Map<String, String>>asList(
        new HashMap<String, String>() {{
          put("cacheManagerName", "myCM1");
          put("cacheName", "aCache1");
        }},
        new HashMap<String, String>() {{
          put("cacheManagerName", "myCM2");
          put("cacheName", "aCache2");
        }},
        new HashMap<String, String>() {{
          put("cacheManagerName", "myCM2");
          put("cacheName", "aCache3");
        }}
    );

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager2.getCache("aCache2", Long.class, String.class).put(2L, "2");
    cacheManager2.getCache("aCache3", Long.class, String.class).put(3L, "3");

    List<Collection<Counter>> allCounters = service.collectStatistics(contextList, "org.ehcache.management.providers.statistics.EhcacheStatisticsProvider", "PutCounter");

    assertThat(allCounters, hasSize(3));

    assertThat(allCounters.get(0), hasSize(1));
    assertThat(allCounters.get(1), hasSize(1));
    assertThat(allCounters.get(2), hasSize(1));

    assertThat(allCounters.get(0).iterator().next().getValue(), equalTo(1L));
    assertThat(allCounters.get(1).iterator().next().getValue(), equalTo(1L));
    assertThat(allCounters.get(2).iterator().next().getValue(), equalTo(1L));
  }

  @Test
  public void testCall() {
    List<Map<String, String>> contextList = Arrays.<Map<String, String>>asList(
        new HashMap<String, String>() {{
          put("cacheManagerName", "myCM1");
          put("cacheName", "aCache1");
        }},
        new HashMap<String, String>() {{
          put("cacheManagerName", "myCM2");
          put("cacheName", "aCache2");
        }}
    );

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager2.getCache("aCache2", Long.class, String.class).put(2L, "2");

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), equalTo("1"));
    assertThat(cacheManager2.getCache("aCache2", Long.class, String.class).get(2L), equalTo("2"));

    List<Object> results = service.callAction(contextList, "org.ehcache.management.providers.actions.EhcacheActionProvider", "clear", new String[0], new Object[0]);
    assertThat(results, hasSize(2));
    assertThat(results.get(0), is(nullValue()));
    assertThat(results.get(1), is(nullValue()));

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), is(Matchers.nullValue()));
    assertThat(cacheManager2.getCache("aCache2", Long.class, String.class).get(2L), is(Matchers.nullValue()));
  }

}
