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
package org.ehcache.management.providers.settings;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.ehcache.CacheManager;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.expiry.Duration;
import org.ehcache.expiry.Expirations;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.management.SharedManagementService;
import org.ehcache.management.config.EhcacheStatisticsProviderConfiguration;
import org.ehcache.management.config.StatisticsProviderConfiguration;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultSharedManagementService;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.context.CapabilityContext;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class EhcacheSettingsProviderTest {

  @ClassRule
  public static TemporaryFolder ROOT = new TemporaryFolder();

  CacheManager cacheManager;
  SharedManagementService sharedManagementService = new DefaultSharedManagementService();
  ObjectMapper mapper = new ObjectMapper();

  @Before
  public void before() {
    mapper.addMixIn(CapabilityContext.class, CapabilityContextMixin.class);
  }

  @After
  public void after() {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void test_standalone_ehcache() throws IOException {
    CacheConfiguration<String, String> cacheConfiguration1 = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB)
            .disk(2, MemoryUnit.MB, true))
        .withExpiry(Expirations.noExpiration())
        .build();

    CacheConfiguration<String, String> cacheConfiguration2 = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB)
            .disk(2, MemoryUnit.MB, true))
        .withExpiry(Expirations.timeToIdleExpiration(Duration.of(2, TimeUnit.HOURS)))
        .build();

    StatisticsProviderConfiguration statisticsProviderConfiguration = new EhcacheStatisticsProviderConfiguration(
        1, TimeUnit.MINUTES,
        100, 1, TimeUnit.SECONDS,
        2, TimeUnit.SECONDS);

    // ehcache cache manager
    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(sharedManagementService)
        .using(new DefaultPersistenceConfiguration(ROOT.newFolder("test_standalone_ehcache")))
        .using(new DefaultManagementRegistryConfiguration()
            .addConfiguration(statisticsProviderConfiguration)
            .setCacheManagerAlias("my-cm-1")
            .addTag("boo")
            .addTags("foo", "baz"))
        .withCache("cache-1", cacheConfiguration1)
        .withCache("cache-2", cacheConfiguration2)
        .build(false);

    cacheManager.init();

    String expected = read("/settings-capability.json");
    String actual = mapper.writeValueAsString(getSettingsCapability()).replaceAll("\\\"cacheManagerDescription\\\":\\\".*\\\",\\\"status\\\"", "\\\"cacheManagerDescription\\\":\\\"\\\",\\\"status\\\"");

    // assertThat for formatted string comparison: ide support is bad
    assertEquals(expected, actual);
  }

  private Capability getSettingsCapability() {
    for (Capability capability : sharedManagementService.getCapabilitiesByContext().values().iterator().next()) {
      if (capability.getName().equals("SettingsCapability")) {
        return capability;
      }
    }
    throw new AssertionError();
  }

  private String read(String path) throws FileNotFoundException {
    Scanner scanner = new Scanner(getClass().getResourceAsStream(path), "UTF-8");
    try {
      return scanner.nextLine();
    } finally {
      scanner.close();
    }
  }

  public static abstract class CapabilityContextMixin {
    @JsonIgnore
    public abstract Collection<String> getRequiredAttributeNames();

    @JsonIgnore
    public abstract Collection<CapabilityContext.Attribute> getRequiredAttributes();
  }

}
