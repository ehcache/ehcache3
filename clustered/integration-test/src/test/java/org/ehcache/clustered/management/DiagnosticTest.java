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

import java.net.URI;
import java.util.Properties;

import static org.ehcache.clustered.client.config.builders.ClusteredResourcePoolBuilder.clusteredDedicated;
import static org.ehcache.config.builders.CacheConfigurationBuilder.newCacheConfigurationBuilder;
import static org.ehcache.config.builders.ResourcePoolsBuilder.newResourcePoolsBuilder;
import static org.hamcrest.core.StringContains.containsString;
import static org.junit.Assert.assertThat;

public class DiagnosticTest extends AbstractClusteringManagementTest {

  private static final String PROP_REQUEST_TIMEOUT = "request.timeout";
  private static final String PROP_REQUEST_TIMEOUTMESSAGE = "request.timeoutMessage";

  @Test
  public void test_state_dump() throws Exception {
    cacheManager.createCache("cache-2", newCacheConfigurationBuilder(
      String.class, String.class,
      newResourcePoolsBuilder()
        .heap(10, EntryUnit.ENTRIES)
        .offheap(1, MemoryUnit.MB)
        .with(clusteredDedicated("primary-server-resource", 2, MemoryUnit.MB)))
      .build());

    Properties properties = new Properties();
    properties.setProperty(ConnectionPropertyNames.CONNECTION_TIMEOUT, String.valueOf("5000"));
    properties.setProperty(ConnectionPropertyNames.CONNECTION_NAME, "diagnostic");
    properties.setProperty(PROP_REQUEST_TIMEOUT, "5000");
    properties.setProperty(PROP_REQUEST_TIMEOUTMESSAGE, "timed out");
    URI uri = URI.create("diagnostic://" + CLUSTER.getConnectionURI().getAuthority());

    Connection connection = ConnectionFactory.connect(uri, properties);
    EntityRef<Diagnostics, Object, Void> ref = connection.getEntityRef(Diagnostics.class, 1, "root");
    Diagnostics diagnostics = ref.fetchEntity(null);

    //TODO: improve these assertions
    // once https://github.com/Terracotta-OSS/terracotta-core/issues/613 and https://github.com/Terracotta-OSS/terracotta-core/pull/601 will be fixed
    // and once the state dump format will be improved.
    String dump = diagnostics.getClusterState();

    // ClusterTierManagerActiveEntity
    assertThat(dump, containsString("clientCount="));
    assertThat(dump, containsString("clientDescriptor="));
    assertThat(dump, containsString("clientIdentifier="));
    assertThat(dump, containsString("attached="));

    // ClusterTierManagerDump
    assertThat(dump, containsString("managerIdentifier="));
    assertThat(dump, containsString("defaultServerResource=primary-server-resource"));
    assertThat(dump, containsString("serverResource="));
    assertThat(dump, containsString("size="));

    // EhcacheStateServiceProvider
    assertThat(dump, containsString("configured=true"));

    // ClusterTierActiveEntity
    assertThat(dump, containsString("storeIdentifier="));

    // ClusterTierDump
    assertThat(dump, containsString("storedKeyType="));
    assertThat(dump, containsString("storedValueType="));
    assertThat(dump, containsString("keySerializerType="));
    assertThat(dump, containsString("valueSerializerType="));
    assertThat(dump, containsString("consistency="));
    assertThat(dump, containsString("resourceName="));
  }

}
