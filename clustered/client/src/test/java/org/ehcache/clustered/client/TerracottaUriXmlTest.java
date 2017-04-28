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

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.core.spi.service.ServiceUtils;
import org.ehcache.xml.XmlConfiguration;
import org.ehcache.xml.exceptions.XmlConfigurationException;
import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

/**
 * TerracottaUriXmlTest
 */
public class TerracottaUriXmlTest {

  @Test
  public void testCanLoadXmlConfigValidForHA() {
    XmlConfiguration xmlConfiguration = new XmlConfiguration(getClass().getResource("/configs/cluster-ha.xml"));
    ClusteringServiceConfiguration config = ServiceUtils.findSingletonAmongst(ClusteringServiceConfiguration.class, xmlConfiguration
      .getServiceCreationConfigurations());
    assertThat(config.getClusterUri().toString(), containsString("example.com:9540,example.com:9640"));
  }

  @Test
  public void testFailsWithInvalidClusterUri() {
    try {
      new XmlConfiguration(getClass().getResource("/configs/cluster-invalid-uri.xml"));
    } catch (XmlConfigurationException e) {
      assertThat(e.getCause().getMessage(), containsString("not facet-valid with respect to pattern"));
    }
  }
}
