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
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.spi.service.ServiceCreationConfiguration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

@RunWith(Parameterized.class)
public class XmlConfigTest {

  @Parameterized.Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{
        {
            "ehcache-management-1.xml",
            new DefaultManagementRegistryConfiguration()
        },
        {
            "ehcache-management-2.xml",
            new DefaultManagementRegistryConfiguration()
                .setCacheManagerAlias("my-cache-manager-name")
                .addTags("webapp-name", "jboss-1", "server-node-1")
        },
        {
            "ehcache-management-3.xml",
            new DefaultManagementRegistryConfiguration()
                .setCacheManagerAlias("my-cache-manager-name")
                .addTags("webapp-name", "jboss-1", "server-node-1")
                .setCollectorExecutorAlias("my-collectorExecutorAlias")
        },
        {
            "ehcache-management-4.xml",
            new DefaultManagementRegistryConfiguration()
                .setCacheManagerAlias("my-cache-manager-name")
                .addTags("webapp-name", "jboss-1", "server-node-1")
        },
        {
            "ehcache-management-5.xml",
            new DefaultManagementRegistryConfiguration()
                .setCacheManagerAlias("my-cache-manager-name")
                .addTags("webapp-name", "jboss-1", "server-node-1")
        }
    });
  }

  private final String xml;
  private final DefaultManagementRegistryConfiguration expectedConfiguration;

  public XmlConfigTest(String xml, DefaultManagementRegistryConfiguration expectedConfiguration) {
    this.xml = xml;
    this.expectedConfiguration = expectedConfiguration;
  }

  @Test
  public void test_config_loaded() throws Exception {
    CacheManager myCacheManager = CacheManagerBuilder.newCacheManager(new XmlConfiguration(getClass().getClassLoader().getResource(xml)));
    myCacheManager.init();
    try {
      DefaultManagementRegistryConfiguration registryConfiguration = null;

      for (ServiceCreationConfiguration<?, ?> configuration : myCacheManager.getRuntimeConfiguration().getServiceCreationConfigurations()) {
        if (configuration instanceof DefaultManagementRegistryConfiguration) {
          registryConfiguration = (DefaultManagementRegistryConfiguration) configuration;
          break;
        }
      }

      assertThat(registryConfiguration, is(not(nullValue())));

      // 1st test: CM alia not set, so generated
      if (xml.endsWith("-1.xml")) {
        expectedConfiguration.setCacheManagerAlias(registryConfiguration.getContext().get("cacheManagerName"));
      }

      assertThat(registryConfiguration.getCacheManagerAlias(), equalTo(expectedConfiguration.getCacheManagerAlias()));
      assertThat(registryConfiguration.getCollectorExecutorAlias(), equalTo(expectedConfiguration.getCollectorExecutorAlias()));
      assertThat(registryConfiguration.getContext(), equalTo(expectedConfiguration.getContext()));
      assertThat(registryConfiguration.getTags(), equalTo(expectedConfiguration.getTags()));

    } finally {
      myCacheManager.close();
    }
  }

}
