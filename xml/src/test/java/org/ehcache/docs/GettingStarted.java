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

import org.ehcache.CacheManager;
import org.ehcache.config.Configuration;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Rule;
import org.junit.Test;
import org.terracotta.org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.net.URL;
import java.time.Duration;

/**
 * GettingStarted
 */
public class GettingStarted {

  @Rule
  public TemporaryFolder tmpDir = new TemporaryFolder();

  @Test
  public void xmlConfigSample() throws Exception {
    // tag::xmlConfig[]
    final URL myUrl = getClass().getResource("/configs/docs/getting-started.xml"); // <1>
    XmlConfiguration xmlConfig = new XmlConfiguration(myUrl); // <2>
    CacheManager myCacheManager = CacheManagerBuilder.newCacheManager(xmlConfig); // <3>
    myCacheManager.init();  // <4>
    // end::xmlConfig[]
  }

  @Test
  public void xmlTemplateSample() throws Exception {
    // tag::xmlTemplate[]
    XmlConfiguration xmlConfiguration = new XmlConfiguration(getClass().getResource("/configs/docs/template-sample.xml"));
    CacheConfigurationBuilder<Long, String> configurationBuilder = xmlConfiguration.newCacheConfigurationBuilderFromTemplate("example", Long.class, String.class); // <1>
    configurationBuilder = configurationBuilder.withResourcePools(ResourcePoolsBuilder.heap(1000)); // <2>
    // end::xmlTemplate[]
  }

  @Test
  public void xmlExpirySample() throws Exception {
    XmlConfiguration xmlConfiguration = new XmlConfiguration(getClass().getResource("/configs/docs/expiry.xml"));
    CacheManagerBuilder.newCacheManager(xmlConfiguration).init();
  }

  @Test
  public void testXmlToString() throws IOException {
    // tag::xmlTranslation[]
    CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
      .with(CacheManagerBuilder.persistence(tmpDir.newFile("myData")))
      .withCache("threeTieredCache",
        CacheConfigurationBuilder.newCacheConfigurationBuilder(Long.class, String.class,
          ResourcePoolsBuilder.newResourcePoolsBuilder()
            .heap(10, EntryUnit.ENTRIES)
            .offheap(1, MemoryUnit.MB)
            .disk(20, MemoryUnit.MB, true))
          .withExpiry(ExpiryPolicyBuilder.timeToIdleExpiration(Duration.ofSeconds(20)))
      ).build(false);

    Configuration configuration = cacheManager.getRuntimeConfiguration();
    XmlConfiguration xmlConfiguration = new XmlConfiguration(configuration);  // <1>
    String xml = xmlConfiguration.toString(); // <2>
    // end::xmlTranslation[]
  }
}
