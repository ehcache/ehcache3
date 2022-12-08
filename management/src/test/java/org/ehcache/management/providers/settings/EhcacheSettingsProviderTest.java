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
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.management.SharedManagementService;
import org.ehcache.management.registry.DefaultManagementRegistryConfiguration;
import org.ehcache.management.registry.DefaultSharedManagementService;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.context.CapabilityContext;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Scanner;

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
        .withExpiry(ExpiryPolicyBuilder.noExpiration())
        .build();

    CacheConfiguration<String, String> cacheConfiguration2 = CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
        newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB)
            .disk(2, MemoryUnit.MB, true))
        .withExpiry(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofHours(2)))
        .build();

    // ehcache cache manager
    DefaultManagementRegistryConfiguration serviceConfiguration = new DefaultManagementRegistryConfiguration()
      .setCacheManagerAlias("my-cm-1")
      .addTag("boo")
      .addTags("foo", "baz");

    cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
        .using(sharedManagementService)
        .using(new DefaultPersistenceConfiguration(ROOT.newFolder("test_standalone_ehcache")))
      .using(serviceConfiguration)
        .withCache("cache-1", cacheConfiguration1)
        .withCache("cache-2", cacheConfiguration2)
        .build(false);

    cacheManager.init();

    String expected = read("/settings-capability.json")
      .replaceAll("instance-id", serviceConfiguration.getInstanceId());
    String actual = mapper.writeValueAsString(getSettingsCapability())
      .replaceAll("\\\"cacheManagerDescription\\\":\\\".*\\\",\\\"instanceId\\\"", "\\\"cacheManagerDescription\\\":\\\"\\\",\\\"instanceId\\\"");

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
    try (Scanner scanner = new Scanner(getClass().getResourceAsStream(path), "UTF-8")) {
      return scanner.nextLine();
    }
  }

  public static abstract class CapabilityContextMixin {
    @JsonIgnore
    public abstract Collection<String> getRequiredAttributeNames();

    @JsonIgnore
    public abstract Collection<CapabilityContext.Attribute> getRequiredAttributes();
  }

}
