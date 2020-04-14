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
package org.ehcache.clustered.management;

import com.terracotta.diagnostic.Diagnostics;
import org.ehcache.config.units.EntryUnit;
import org.ehcache.config.units.MemoryUnit;
import org.junit.Test;
import org.terracotta.connection.Connection;
import org.terracotta.connection.ConnectionFactory;
import org.terracotta.connection.ConnectionPropertyNames;
import org.terracotta.connection.entity.EntityRef;
import org.terracotta.management.model.cluster.Server;

import java.net.URI;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;

public class DiagnosticTest extends AbstractClusteringManagementTest {

  private static final String PROP_REQUEST_TIMEOUT = "request.timeout";
  private static final String PROP_REQUEST_TIMEOUTMESSAGE = "request.timeoutMessage";

  @Test
  public void test_CACHE_MANAGER_CLOSED() throws Exception {
    cacheManager.createCache("cache-2", newCacheConfigurationBuilder(
      String.class, String.class,
      newResourcePoolsBuilder()
        .heap(10, EntryUnit.ENTRIES)
        .offheap(1, MemoryUnit.MB)
        .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
      .build());

    int activePort = readTopology().serverStream().filter(Server::isActive).findFirst().get().getBindPort();

    Properties properties = new Properties();
    properties.setProperty(ConnectionPropertyNames.CONNECTION_TIMEOUT, String.valueOf("10000"));
    properties.setProperty(ConnectionPropertyNames.CONNECTION_NAME, "diagnostic");
    properties.setProperty(PROP_REQUEST_TIMEOUT, "10000");
    properties.setProperty(PROP_REQUEST_TIMEOUTMESSAGE, "timed out");
    URI uri = URI.create("diagnostic://localhost:" + activePort);

    Connection connection = ConnectionFactory.connect(uri, properties);
    EntityRef<Diagnostics, Object, Void> ref = connection.getEntityRef(Diagnostics.class, 1, "root");
    Diagnostics diagnostics = ref.fetchEntity(null);

    //TODO: improve these assertions
    // once https://github.com/Terracotta-OSS/terracotta-core/issues/613 and https://github.com/Terracotta-OSS/terracotta-core/pull/601 will be fixed
    // and once the state dump format will be improved.
    String dump = diagnostics.getClusterState();

    // ClusterTierManagerActiveEntity
    assertThat(dump).contains("clientCount=");
    assertThat(dump).contains("clientDescriptor=");

    // ClusterTierManagerDump
    assertThat(dump).contains("managerIdentifier=");
    assertThat(dump).contains("defaultServerResource=primary-server-resource");
    assertThat(dump).contains("serverResource=");
    assertThat(dump).contains("size=");

    // EhcacheStateServiceProvider
    assertThat(dump).contains("configured=true");

    // ClusterTierDump
    assertThat(dump).contains("storedKeyType=");
    assertThat(dump).contains("storedValueType=");
    assertThat(dump).contains("keySerializerType=");
    assertThat(dump).contains("valueSerializerType=");
    assertThat(dump).contains("consistency=");
    assertThat(dump).contains("resourceName=");
  }

}
