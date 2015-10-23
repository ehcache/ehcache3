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
package org.ehcache.docs;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.CacheManagerBuilder;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.CacheConfigurationBuilder;
import org.ehcache.config.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.management.ManagementRegistry;
import org.ehcache.management.SharedManagementService;
import org.ehcache.management.registry.DefaultManagementRegistry;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultSharedManagementService;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;
import org.terracotta.management.capabilities.Capability;
import org.terracotta.management.capabilities.context.CapabilityContext;
import org.terracotta.management.capabilities.descriptors.Descriptor;
import org.terracotta.management.context.ContextContainer;
import org.terracotta.management.stats.Statistic;
import org.terracotta.management.stats.primitive.Counter;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;


/**
 * @author Ludovic Orban
 */
public class ManagementTest {

  @Test
  public void usingManagementRegistry() throws Exception {
    // tag::usingManagementRegistry[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    DefaultManagementRegistryConfiguration registryConfiguration = new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager"); // <1>
    ManagementRegistry managementRegistry = new DefaultManagementRegistry(registryConfiguration); // <2>
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry) // <3>
        .build(true);


    Cache<Long, String> aCache = cacheManager.getCache("aCache", Long.class, String.class);
    aCache.get(0L); // <4>
    aCache.get(0L);
    aCache.get(0L);

    Map<String, String> context = createContext(managementRegistry); // <5>

    Collection<Statistic<?>> counters = managementRegistry.collectStatistics(context, "StatisticsCapability", "GetCounter"); // <6>
    Assert.assertThat(counters.size(), Matchers.is(1));
    Counter getCounter = (Counter) counters.iterator().next();

    Assert.assertThat(getCounter.getValue(), Matchers.equalTo(3L)); // <7>

    cacheManager.close();
    // end::usingManagementRegistry[]
  }

  @Test
  public void capabilitiesAndContexts() throws Exception {
    // tag::capabilitiesAndContexts[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry();
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);


    Collection<Capability> capabilities = managementRegistry.getCapabilities(); // <1>
    Assert.assertThat(capabilities.isEmpty(), Matchers.is(false));
    Capability capability = capabilities.iterator().next();
    String capabilityName = capability.getName(); // <2>
    Collection<Descriptor> capabilityDescriptions = capability.getDescriptions(); // <3>
    Assert.assertThat(capabilityDescriptions.isEmpty(), Matchers.is(false));
    CapabilityContext capabilityContext = capability.getCapabilityContext();
    Collection<CapabilityContext.Attribute> attributes = capabilityContext.getAttributes(); // <4>
    Assert.assertThat(attributes.size(), Matchers.is(2));
    Iterator<CapabilityContext.Attribute> iterator = attributes.iterator();
    CapabilityContext.Attribute attribute1 = iterator.next();
    Assert.assertThat(attribute1.getName(), Matchers.equalTo("cacheManagerName"));  // <5>
    Assert.assertThat(attribute1.isRequired(), Matchers.is(true));
    CapabilityContext.Attribute attribute2 = iterator.next();
    Assert.assertThat(attribute2.getName(), Matchers.equalTo("cacheName")); // <6>
    Assert.assertThat(attribute2.isRequired(), Matchers.is(true));

    ContextContainer contextContainer = managementRegistry.getContext();  // <7>
    Assert.assertThat(contextContainer.getName(), Matchers.equalTo("cacheManagerName"));  // <8>
    Assert.assertThat(contextContainer.getValue(), Matchers.startsWith("cache-manager-"));
    Collection<ContextContainer> subContexts = contextContainer.getSubContexts();
    Assert.assertThat(subContexts.size(), Matchers.is(1));
    ContextContainer subContextContainer = subContexts.iterator().next();
    Assert.assertThat(subContextContainer.getName(), Matchers.equalTo("cacheName"));  // <9>
    Assert.assertThat(subContextContainer.getValue(), Matchers.equalTo("aCache"));


    cacheManager.close();
    // end::capabilitiesAndContexts[]
  }

  @Test
  public void actionCall() throws Exception {
    // tag::actionCall[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    ManagementRegistry managementRegistry = new DefaultManagementRegistry();
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(managementRegistry)
        .build(true);

    Cache<Long, String> aCache = cacheManager.getCache("aCache", Long.class, String.class);
    aCache.put(0L, "zero"); // <1>

    Map<String, String> context = createContext(managementRegistry); // <2>

    managementRegistry.callAction(context, "ActionsCapability", "clear", new String[0], new Object[0]); // <3>

    Assert.assertThat(aCache.get(0L), Matchers.is(Matchers.nullValue())); // <4>

    cacheManager.close();
    // end::actionCall[]
  }

  @Test
  public void managingMultipleCacheManagers() throws Exception {
    // tag::managingMultipleCacheManagers[]
    CacheConfiguration<Long, String> cacheConfiguration = CacheConfigurationBuilder.newCacheConfigurationBuilder()
        .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder().heap(10, EntryUnit.ENTRIES).build())
        .buildConfig(Long.class, String.class);

    SharedManagementService sharedManagementService = new DefaultSharedManagementService(); // <1>
    CacheManager cacheManager1 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(new DefaultManagementRegistryConfiguration().setCacheManagerAlias("myCacheManager"))
        .using(sharedManagementService) // <2>
        .build(true);

    CacheManager cacheManager2 = CacheManagerBuilder.newCacheManagerBuilder()
        .withCache("aCache", cacheConfiguration)
        .using(sharedManagementService) // <3>
        .build(true);

    Map<String, String> context = new HashMap<String, String>();
    context.put("cacheManagerName", "myCacheManager");
    context.put("cacheName", "aCache");

    List<Collection<Statistic<?>>> counters = sharedManagementService.collectStatistics(Arrays.asList(context), "StatisticsCapability", "GetCounter");

    cacheManager2.close();
    cacheManager1.close();
    // end::managingMultipleCacheManagers[]
  }

  private static Map<String, String> createContext(ManagementRegistry managementRegistry) {
    Map<String, String> result = new HashMap<String, String>();
    ContextContainer cmCtx = managementRegistry.getContext();
    ContextContainer firstCacheCtx = cmCtx.getSubContexts().iterator().next();

    result.put(cmCtx.getName(), cmCtx.getValue());
    result.put(firstCacheCtx.getName(), firstCacheCtx.getValue());
    return result;
  }

}
