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
import org.junit.Before;
import org.junit.Test;
import org.terracotta.connection.ConnectionPropertyNames;

import java.net.URI;
import java.util.Collections;

import static org.junit.Assert.*;

public class DefaultClusteringServiceTest {

  @Before
  public void resetPassthroughServer() throws Exception {
    UnitTestConnectionService.reset();
  }

  @Test
  public void testConnectionName() throws Exception {
    String entityIdentifier = "my-application";
    ClusteringServiceConfiguration configuration =
        new ClusteringServiceConfiguration(
            URI.create("http://example.com:9540/" + entityIdentifier + "?auto-create"),
            null,
            Collections.<String, ClusteringServiceConfiguration.PoolDefinition>emptyMap());
    DefaultClusteringService service = new DefaultClusteringService(configuration);
    service.start(null);

    assertEquals(
        UnitTestConnectionService.getConnectionProperties().getProperty(ConnectionPropertyNames.CONNECTION_NAME),
        DefaultClusteringService.CONNECTION_PREFIX + entityIdentifier
    );
  }
}