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

package org.ehcache.clustered.client.service;

import org.ehcache.clustered.client.config.ClusteringServiceConfiguration;
import org.ehcache.clustered.client.internal.UnitTestConnectionService;
import org.ehcache.clustered.client.internal.UnitTestConnectionService.PassthroughServerBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.terracotta.connection.ConnectionPropertyNames;

import java.net.URI;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class DefaultClusteringServiceTest {

  private static final String CLUSTER_URI_BASE = "http://example.com:9540/";

  @Before
  public void definePassthroughServer() throws Exception {
    UnitTestConnectionService.add(CLUSTER_URI_BASE,
        new PassthroughServerBuilder()
            .build());
  }

  @After
  public void removePassthroughServer() throws Exception {
    UnitTestConnectionService.remove(CLUSTER_URI_BASE);
  }

  @Test
  public void testConnectionName() throws Exception {
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(
            URI.create(CLUSTER_URI_BASE + entityIdentifier + "?auto-create"),
            null,
            Collections.<String, ClusteringServiceConfiguration.PoolDefinition>emptyMap());
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    Collection<Properties> propsCollection = UnitTestConnectionService.getConnectionProperties(URI.create(CLUSTER_URI_BASE));
    assertThat(propsCollection.size(), is(1));
    Properties props = propsCollection.iterator().next();
    assertEquals(
        props.getProperty(ConnectionPropertyNames.CONNECTION_NAME),
        DefaultClusteringService.CONNECTION_PREFIX + entityIdentifier
    );
  }
}