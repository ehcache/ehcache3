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
package org.ehcache.clustered.client.config;

import org.ehcache.clustered.common.Consistency;
import org.ehcache.config.Configuration;
import org.ehcache.xml.XmlConfiguration;
import org.junit.Test;

import java.net.URI;

import static org.ehcache.core.spi.service.ServiceUtils.findSingletonAmongst;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ClusteredConfigurationDerivationTest {
  private static final String SIMPLE_CLUSTER_XML = "/configs/simple-cluster.xml";
  private static final URI UPDATED_CLUSTER_URI = URI.create("terracotta://updated.example.com:9540/cachemanager");

  @Test
  public void testUpdateUri() throws Exception {
    final XmlConfiguration configuration = new XmlConfiguration(this.getClass().getResource(SIMPLE_CLUSTER_XML));

    Configuration newServer = configuration.derive().updateServices(ClusteringServiceConfiguration.class, existing ->
      existing.usingUri(UPDATED_CLUSTER_URI)).build();
    assertThat(findSingletonAmongst(ClusteringServiceConfiguration.class, newServer.getServiceCreationConfigurations()).getClusterUri(), is(UPDATED_CLUSTER_URI));
  }

  @Test
  public void testAddConsistency() {
    final XmlConfiguration configuration = new XmlConfiguration(this.getClass().getResource(SIMPLE_CLUSTER_XML));

    Configuration newConsistency = configuration.derive().updateCache("simple-cache", existing ->
      existing.withService(new ClusteredStoreConfiguration(Consistency.STRONG))).build();
    assertThat(findSingletonAmongst(ClusteredStoreConfiguration.class, newConsistency.getCacheConfigurations().get("simple-cache").getServiceConfigurations()).getConsistency(), is(Consistency.STRONG));
  }
}
