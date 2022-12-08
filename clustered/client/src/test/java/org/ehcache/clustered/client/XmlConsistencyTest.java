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

package org.ehcache.clustered.client;

import org.ehcache.clustered.client.config.ClusteredStoreConfiguration;
import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.CacheConfiguration;
import org.ehcache.config.Configuration;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * XmlConsistencyTest
 */
public class XmlConsistencyTest {

  private Configuration xmlConfiguration;

  @Before
  public void setUp() {
    xmlConfiguration = new XmlConfiguration(this.getClass().getResource("/configs/consistency.xml"));
  }

  @Test
  public void testDefaultConsistency() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = xmlConfiguration.getCacheConfigurations().get("default-consistency-cache");
    ClusteredStoreConfiguration clusteredStoreConfiguration = ServiceUtils.findSingletonAmongst(ClusteredStoreConfiguration.class, cacheConfiguration
        .getServiceConfigurations());

    assertNull(clusteredStoreConfiguration);
  }

  @Test
  public void testEventualConsistency() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = xmlConfiguration.getCacheConfigurations().get("eventual-consistency-cache");
    ClusteredStoreConfiguration clusteredStoreConfiguration = ServiceUtils.findSingletonAmongst(ClusteredStoreConfiguration.class, cacheConfiguration
        .getServiceConfigurations());

    assertThat(clusteredStoreConfiguration.getConsistency(), is(Consistency.EVENTUAL));
  }

  @Test
  public void testStrongConsistency() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = xmlConfiguration.getCacheConfigurations().get("strong-consistency-cache");
    ClusteredStoreConfiguration clusteredStoreConfiguration = ServiceUtils.findSingletonAmongst(ClusteredStoreConfiguration.class, cacheConfiguration
        .getServiceConfigurations());

    assertThat(clusteredStoreConfiguration.getConsistency(), is(Consistency.STRONG));
  }

  @Test
  public void testTemplateConsistency() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = xmlConfiguration.getCacheConfigurations().get("template-consistency-cache");
    ClusteredStoreConfiguration clusteredStoreConfiguration = ServiceUtils.findSingletonAmongst(ClusteredStoreConfiguration.class, cacheConfiguration
        .getServiceConfigurations());

    assertThat(clusteredStoreConfiguration.getConsistency(), is(Consistency.STRONG));
  }

  @Test
  public void testTemplateConsistencyOverride() throws Exception {
    CacheConfiguration<?, ?> cacheConfiguration = xmlConfiguration.getCacheConfigurations().get("template-override-consistency-cache");
    ClusteredStoreConfiguration clusteredStoreConfiguration = ServiceUtils.findSingletonAmongst(ClusteredStoreConfiguration.class, cacheConfiguration
        .getServiceConfigurations());

    assertThat(clusteredStoreConfiguration.getConsistency(), is(Consistency.EVENTUAL));
  }
}
