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
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.management.ManagementRegistryServiceConfiguration;
import org.ehcache.management.SharedManagementService;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.terracotta.management.call.ContextualReturn;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.context.Context;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.registry.ResultSet;
import org.terracotta.management.stats.ContextualStatistics;
import org.terracotta.management.stats.primitive.Counter;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.NoSuchElementException;

import static org.ehcache.config.builders.ResourcePoolsBuilder.heap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Mathieu Carbou
 */
@RunWith(JUnit4.class)
public class DefaultSharedManagementServiceTest {

  CacheManager cacheManager1;
  CacheManager cacheManager2;
  SharedManagementService service;

  ManagementRegistryServiceConfiguration config1;
  ManagementRegistryServiceConfiguration config2;

  @Before
  public void init() {
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();

    service = new DefaultSharedManagementService();

    cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache1", cacheConfiguration)
        .using(service)
        .using(config1 = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM1"))
        .build(true);

    cacheManager2 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache2", cacheConfiguration)
        .withCache("aCache3", cacheConfiguration)
        .using(service)
        .using(config2 = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCM2"))
        .build(true);

    // this serie of calls make sure the registry still works after a full init / close / init loop
    cacheManager1.close();
    cacheManager1.init();
    cacheManager2.close();
    cacheManager2.init();
  }

  @After()
  public void close() {
    cacheManager2.close();
    cacheManager1.close();
  }

  @Test
  public void testSharedContexts() {
    assertEquals(2, service.getContextContainers().size());

    ContextContainer contextContainer1 = service.getContextContainers().get(config1.getContext());
    ContextContainer contextContainer2 = service.getContextContainers().get(config2.getContext());

    assertThat(contextContainer1, is(notNullValue()));
    assertThat(contextContainer2, is(notNullValue()));

    assertThat(contextContainer1.getName(), equalTo("cacheManagerName"));
    assertThat(contextContainer1.getValue(), equalTo("myCM1"));

    assertThat(contextContainer2.getName(), equalTo("cacheManagerName"));
    assertThat(contextContainer2.getValue(), equalTo("myCM2"));

    assertThat(contextContainer1.getSubContexts().size(), equalTo(1));
    assertThat(contextContainer1.getSubContexts().iterator().next().getName(), equalTo("cacheName"));
    assertThat(contextContainer1.getSubContexts().iterator().next().getValue(), equalTo("aCache1"));

    assertThat(contextContainer2.getSubContexts().size(), equalTo(2));
    assertThat(contextContainer2.getSubContexts().iterator().next().getName(), equalTo("cacheName"));
    assertThat(new ArrayList<ContextContainer>(contextContainer2.getSubContexts()).get(1).getName(), equalTo("cacheName"));

    assertThat(new ArrayList<ContextContainer>(contextContainer2.getSubContexts()).get(0).getValue(), isIn(Arrays.asList("aCache2", "aCache3")));
    assertThat(new ArrayList<ContextContainer>(contextContainer2.getSubContexts()).get(1).getValue(), isIn(Arrays.asList("aCache2", "aCache3")));
  }

  @Test
  public void testSharedCapabilities() {
    assertEquals(2, service.getCapabilities().size());

    Collection<Capability> capabilities1 = service.getCapabilities().get(config1.getContext());
    Collection<Capability> capabilities2 = service.getCapabilities().get(config2.getContext());

    assertThat(capabilities1, hasSize(3));
    assertThat(new ArrayList<Capability>(capabilities1).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(capabilities1).get(1).getName(), equalTo("StatisticsCapability"));

    assertThat(capabilities2, hasSize(3));
    assertThat(new ArrayList<Capability>(capabilities2).get(0).getName(), equalTo("ActionsCapability"));
    assertThat(new ArrayList<Capability>(capabilities2).get(1).getName(), equalTo("StatisticsCapability"));
  }

  @Test
  public void testStats() {
    List<Context> contextList = Arrays.asList(
        Context.empty()
            .with("cacheManagerName", "myCM1")
            .with("cacheName", "aCache1"),
        Context.empty()
            .with("cacheManagerName", "myCM2")
            .with("cacheName", "aCache2"),
        Context.empty()
            .with("cacheManagerName", "myCM2")
            .with("cacheName", "aCache3"));

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager2.getCache("aCache2", Long.class, String.class).put(2L, "2");
    cacheManager2.getCache("aCache3", Long.class, String.class).put(3L, "3");

    ResultSet<ContextualStatistics> allCounters = service.withCapability("StatisticsCapability")
        .queryStatistic("PutCounter")
        .on(contextList)
        .build()
        .execute();

    assertThat(allCounters.size(), equalTo(3));

    assertThat(allCounters.getResult(contextList.get(0)).size(), equalTo(1));
    assertThat(allCounters.getResult(contextList.get(1)).size(), equalTo(1));
    assertThat(allCounters.getResult(contextList.get(2)).size(), equalTo(1));

    assertThat(allCounters.getResult(contextList.get(0)).getStatistic(Counter.class).getValue(), equalTo(1L));
    assertThat(allCounters.getResult(contextList.get(1)).getStatistic(Counter.class).getValue(), equalTo(1L));
    assertThat(allCounters.getResult(contextList.get(2)).getStatistic(Counter.class).getValue(), equalTo(1L));
  }

  @Test
  public void testCall() {
    List<Context> contextList = Arrays.asList(
        Context.empty()
            .with("cacheManagerName", "myCM1")
            .with("cacheName", "aCache1"),
        Context.empty()
            .with("cacheManagerName", "myCM1")
            .with("cacheName", "aCache4"),
        Context.empty()
            .with("cacheManagerName", "myCM2")
            .with("cacheName", "aCache2"),
        Context.empty()
            .with("cacheManagerName", "myCM55")
            .with("cacheName", "aCache55"));

    cacheManager1.getCache("aCache1", Long.class, String.class).put(1L, "1");
    cacheManager2.getCache("aCache2", Long.class, String.class).put(2L, "2");

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), equalTo("1"));
    assertThat(cacheManager2.getCache("aCache2", Long.class, String.class).get(2L), equalTo("2"));

    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class, heap(10))
        .build();
    cacheManager1.createCache("aCache4", cacheConfiguration);

    cacheManager1.getCache("aCache4", Long.class, String.class).put(4L, "4");
    assertThat(cacheManager1.getCache("aCache4", Long.class, String.class).get(4L), equalTo("4"));

    ResultSet<ContextualReturn<Serializable>> results = service.withCapability("ActionsCapability")
        .call("clear")
        .on(contextList)
        .build()
        .execute();

    assertThat(results.size(), Matchers.equalTo(4));

    assertThat(results.getResult(contextList.get(0)).hasValue(), is(true));
    assertThat(results.getResult(contextList.get(1)).hasValue(), is(true));
    assertThat(results.getResult(contextList.get(2)).hasValue(), is(true));
    assertThat(results.getResult(contextList.get(3)).hasValue(), is(false));

    assertThat(results.getResult(contextList.get(0)).getValue(), is(nullValue()));
    assertThat(results.getResult(contextList.get(1)).getValue(), is(nullValue()));
    assertThat(results.getResult(contextList.get(2)).getValue(), is(nullValue()));

    try {
      results.getResult(contextList.get(3)).getValue();
      fail();
    } catch (Exception e) {
      assertThat(e, instanceOf(NoSuchElementException.class));
    }

    assertThat(cacheManager1.getCache("aCache1", Long.class, String.class).get(1L), is(Matchers.nullValue()));
    assertThat(cacheManager2.getCache("aCache2", Long.class, String.class).get(2L), is(Matchers.nullValue()));
    assertThat(cacheManager1.getCache("aCache4", Long.class, String.class).get(4L), is(Matchers.nullValue()));
  }

}
