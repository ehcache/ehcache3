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
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.terracotta.json.DefaultJsonFactory;
import org.terracotta.json.Json;
import org.terracotta.json.gson.GsonModule;
import org.terracotta.management.model.capabilities.Capability;
import org.terracotta.management.model.capabilities.descriptors.Descriptor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;

import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.junit.Assert.assertEquals;

@RunWith(JUnit4.class)
public class EhcacheSettingsProviderTest {

  @ClassRule
  public static TemporaryFolder ROOT = new TemporaryFolder();

  CacheManager cacheManager;
  SharedManagementService sharedManagementService = new DefaultSharedManagementService();
  Json mapper = new DefaultJsonFactory()
    .withModule((GsonModule) config -> config.serializeSubtypes(Descriptor.class))
    .pretty()
    .create();

  @After
  public void after() {
    if (cacheManager != null) {
      cacheManager.close();
    }
  }

  @Test
  public void test_standalone_ehcache() throws IOException, URISyntaxException {
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

    String expected = read("/settings-capability.json").trim();
    String actual = mapper.toString(getSettingsCapability())
      .replaceAll("\"instanceId\": \"[0-9a-f-]+\"", "\"instanceId\": \"\"");

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

  private String read(String resource) throws IOException, URISyntaxException {
    return new String(Files.readAllBytes(Paths.get(getClass().getResource(resource).toURI())), StandardCharsets.UTF_8)
      .replaceAll("\r", "");
  }
}
